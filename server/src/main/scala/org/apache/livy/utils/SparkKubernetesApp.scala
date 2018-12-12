/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.livy.utils
import java.lang
import java.util.concurrent.TimeoutException
import io.fabric8.kubernetes.api.model.{Pod, PodList, PodStatus}
import io.fabric8.kubernetes.client._
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.livy.LivyConf.Entry
import org.apache.livy.{LivyConf, Logging, Utils}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object SparkKubernetesApp extends Logging {
  import KubernetesConfig._

  def init(livyConf: LivyConf): Unit = {
    master = livyConf.sparkMaster()
    logSize = livyConf.getInt(LivyConf.SPARK_LOGS_SIZE)
    // TODO add to LivyConf
    sessionLeakageCheckInterval = livyConf.getTimeAsMs(Entry("livy.server.kubernetes.app-leakage.check-interval", "600s"))
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(Entry("livy.server.kubernetes.app-leakage.check-timeout", "60s"))
    logger.info(
      s"Initialized SparkKubernetesApp: master=[$master] " +
        s"| sessionLeakageCheckInterval=[$sessionLeakageCheckInterval] " +
        s"| sessionLeakageCheckTimeout=[$sessionLeakageCheckTimeout]"
    )
    leakedAppsGCThread.setDaemon(true)
    leakedAppsGCThread.setName("KubernetesLeakedAppsGCThread")
    leakedAppsGCThread.start()
  }

  private val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()
  private var sessionLeakageCheckTimeout : Long   = _
  private var sessionLeakageCheckInterval: Long   = _
  private var master                     : String = _
  private var logSize                    : Int    = _
  // KubernetesClient is thread safe. Create once, share it across threads.
  lazy val kubernetesClient: DefaultKubernetesClient = {
    val config = new ConfigBuilder()
      .withApiVersion("v1")
      //      .withMasterUrl("https://kubernetes.default.svc")
      //      .withOauthToken(Files.toString(new File(KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH), Charsets.UTF_8))
      //      .withCaCertFile(KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)
      .build()
    new DefaultKubernetesClient(config)
  }
  private def getKubernetesTagToAppIdTimeout(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(Entry("livy.server.kubernetes.app-lookup-timeout", "120s")) milliseconds
  private def getKubernetesPollInterval(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(Entry("livy.server.kubernetes.poll-interval", "5s")) milliseconds
  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threashold
          val iter = leakedAppTags.entrySet().iterator()
          var isRemoved = false
          val now = System.currentTimeMillis()
          val sparkDriverPods = kubernetesClient
            .pods.inAnyNamespace
            .withLabel(KUBERNETES_SPARK_ROLE_LABEL, KUBERNETES_SPARK_ROLE_DRIVER)
            .withLabel(KUBERNETES_SPARK_APP_TAG_LABEL)
            .list().getItems.asScala
          while (iter.hasNext) {
            val entry = iter.next()
            sparkDriverPods.find(_.getMetadata.getLabels.get(KUBERNETES_SPARK_APP_TAG_LABEL) == entry.getKey)
              .foreach({
                pod =>
                  val sparkAppTag = pod.getMetadata.getLabels.get(KUBERNETES_SPARK_APP_TAG_LABEL)
                  info(s"Kill leaked app $sparkAppTag")
                  isRemoved = kubernetesClient.pods.delete(pod)
                  iter.remove()
              })
            if (!isRemoved) {
              if ((entry.getValue - now) > sessionLeakageCheckTimeout) {
                iter.remove()
                info(s"Remove leaked kubernetes app tag ${entry.getKey}")
              }
            }
          }
        }
        Thread.sleep(sessionLeakageCheckInterval)
      }
    }
  }
}
/**
  * Provide a class to control a Spark application using Kubernetes API.
  *
  * @param appTag      An app tag that can unique identify the Spark Kubernetes app.
  * @param appIdOption The appId of the Spark Kubernetes app. If this's None, Spark Kubernetes App will find it
  *                    using appTag.
  * @param process     The spark-submit process launched the Spark Kubernetes application. This is optional.
  *                    If it's provided, Spark Kubernetes log() will include its log.
  * @param listener    Optional listener for notification of appId discovery and app state changes.
  */
class SparkKubernetesApp private[utils](
                                         appTag: String,
                                         appIdOption: Option[String],
                                         process: Option[LineBufferedProcess],
                                         listener: Option[SparkAppListener],
                                         livyConf: LivyConf,
                                         kubernetesClient: => KubernetesClient = SparkKubernetesApp.kubernetesClient) // For unit test. TODO ???
  extends SparkApp
    with Logging {
  import KubernetesConfig._
  import SparkKubernetesApp._
  private        val appIdPromise         : Promise[String]    = Promise()
  private[utils] var state                : SparkApp.State     = SparkApp.State.STARTING
  private        var kubernetesDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]
  private        var kubernetesAppLog     : IndexedSeq[String] = IndexedSeq.empty[String]
  // TODO ???
  override def log(): IndexedSeq[String] =
  ("stdout: " +: kubernetesAppLog) ++
    ("\nstderr: " +: (process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++ process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String]))) ++
    ("\nKubernetes Diagnostics: " +: kubernetesDiagnostics)
  override def kill(): Unit = synchronized {
    if (isRunning) {
      try {
        kubernetesClient.selectSparkDrivers()
          .withLabel(KUBERNETES_SPARK_APP_ID_LABEL, appIdOption.get) // TODO callGetAppId to catch errors when appId is not defined
          .delete()
      } catch {
        // TODO adopt to Kubernetes
        // We cannot kill the YARN app without the app id.
        // There's a chance the YARN app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException =>
          warn("Deleting a session while its Kubernetes application is not found.")
          kubernetesAppMonitorThread.interrupt()
      } finally {
        process.foreach(_.destroy())
      }
    }
  }
  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }
  /**
    * Find the corresponding YARN application id from an application tag.
    *
    * @param appTag The application tag tagged on the target application.
    *               If the tag is not unique, it returns the first application it found.
    *               It will be converted to lower case to match YARN's behaviour.
    *
    * @return ApplicationId or the failure.
    */
  @tailrec
  private def getAppIdFromTag(
                               appTag: String,
                               pollInterval: Duration,
                               deadline: Deadline): Option[String] = {
    val driver = kubernetesClient.getSparkDriverByAppTag(appTag)
    if (driver.isDefined) {
      Option(driver.get.getMetadata.getLabels.get(KUBERNETES_SPARK_APP_ID_LABEL))
    } else {
      if (deadline.isOverdue) {
        process.foreach(_.destroy())
        leakedAppTags.put(appTag, System.currentTimeMillis())
        throw new Exception(s"No Kubernetes application is found with tag $appTag in " +
          livyConf.getTimeAsMs(Entry("livy.server.kubernetes.app-lookup-timeout", "120s")) / 1000 + " seconds. " +
          "Please check your cluster status, it is may be very busy.")
      } else {
        Clock.sleep(pollInterval.toMillis)
        getAppIdFromTag(appTag, pollInterval, deadline)
      }
    }
  }
  private def isRunning: Boolean = {
    import SparkApp.State._
    !Seq(FAILED, FINISHED, KILLED).contains(state)
  }
  // Exposed for unit test.
  private[utils] def mapKubernetesState(kubernetesPodStatus: Option[PodStatus]): SparkApp.State.Value = {
    val state = Try(kubernetesPodStatus.get.getPhase.toLowerCase).getOrElse("error")
    state match {
      case "pending" | "containercreating" => SparkApp.State.STARTING
      case "running"                       => SparkApp.State.RUNNING
      case "completed"                     => SparkApp.State.FINISHED
      case "failed" | "error"              => SparkApp.State.FAILED
      case _                               => SparkApp.State.KILLED
    }
  }
  // Exposed for unit test.
  // TODO Instead of spawning a thread for every session, create a centralized thread and
  // batch Kubernetes queries.
  private[utils] val kubernetesAppMonitorThread = Utils.startDaemonThread(s"kubernetsAppMonitorThread-$this") {
    try {
      // If appId is not known, query Kubernetes by appTag to get it.
      val appId = try {
        appIdOption.getOrElse {
          val pollInterval = getKubernetesPollInterval(livyConf)
          val deadline = getKubernetesTagToAppIdTimeout(livyConf).fromNow
          getAppIdFromTag(appTag, pollInterval, deadline).get
        }
      } catch {
        case e: Exception =>
          appIdPromise.failure(e)
          throw e
      }
      appIdPromise.success(appId)
      Thread.currentThread().setName(s"kuberbetesAppMonitorThread-$appId")
      listener.foreach(_.appIdKnown(appId.toString))
      val pollInterval = getKubernetesPollInterval(livyConf)
      var appInfo = AppInfo()
      while (isRunning) {
        try {
          Clock.sleep(pollInterval.toMillis)
          // Refresh application state
          val sparkPods = kubernetesClient.getSparkPodsByAppId(appId)
          val driverPodOption = sparkPods.find(_.getMetadata.getLabels.get(KUBERNETES_SPARK_ROLE_LABEL) == KUBERNETES_SPARK_ROLE_DRIVER)
          // TODO reformat output
          kubernetesDiagnostics = sparkPods
            .sortBy(pod ⇒ pod.getMetadata.getName)
            .map(buildSparkPodDiagnosticsPrettyString)
            .flatMap(_.split("\n"))
            .toIndexedSeq
          changeState(mapKubernetesState(Try(driverPodOption.get.getStatus).toOption))
          // Refresh app log cache
          kubernetesAppLog = Try({
            val driverPod = driverPodOption.get
            val name = driverPod.getMetadata.getName
            val namespace = driverPod.getMetadata.getNamespace
            kubernetesClient
              .pods.inNamespace(namespace).withName(name)
              .tailingLines(logSize).getLog.split("\n").toIndexedSeq
          }).getOrElse(IndexedSeq("No log..."))
          val latestAppInfo = {
            val historyServerOption = Option(System.getenv("HISTORY_SERVER_ENDPOINT"))
            val historyServerInfo = if (historyServerOption.isDefined) Option(s"${historyServerOption.get}/history/$appId/jobs/") else None
            val driverMetadata = Try(driverPodOption.get.getMetadata)
            val sparkUiInfo = if (driverMetadata.isSuccess) {
              val meta = driverMetadata.get
              Option(s"${Option(System.getenv("LIVY_UI_PROXY_URL")).getOrElse("")}/${meta.getNamespace}/${meta.getName}-svc/jobs/")
            } else {
              None
            }
            AppInfo(historyServerInfo, sparkUiInfo)
          }
          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
        } catch {
          // This exception might be thrown during app is starting up. It's transient.
          case e: ApplicationAttemptNotFoundException =>
          // Workaround YARN-4411: No enum constant FINAL_SAVING from getApplicationAttemptReport()
          case e: IllegalArgumentException =>
            if (e.getMessage.contains("FINAL_SAVING")) {
              debug("Encountered YARN-4411.")
            } else {
              throw e
            }
        }
      }
    } catch {
      case e: InterruptedException =>
        kubernetesDiagnostics = ArrayBuffer("Session stopped by user.")
        changeState(SparkApp.State.KILLED)
      case e: Throwable            =>
        error(s"Error whiling refreshing Kubernetes state: $e")
        kubernetesDiagnostics = ArrayBuffer(e.toString +: e.getStackTrace.map(_.toString): _*)
        changeState(SparkApp.State.FAILED)
    }
  }

  implicit class KubernetesClientExtensions(client: KubernetesClient) {
    def selectSparkDrivers(sparkRoleLabel: String = KUBERNETES_SPARK_ROLE_LABEL, sparkRoleDriver: String = KUBERNETES_SPARK_ROLE_DRIVER): FilterWatchListDeletable[Pod, PodList, lang.Boolean, Watch, Watcher[Pod]] =
      client.pods().inAnyNamespace().withLabel(sparkRoleLabel, sparkRoleDriver)
    def getSparkDrivers(sparkAppIdLabel: String = KUBERNETES_SPARK_APP_ID_LABEL, sparkAppTagLabel: String = KUBERNETES_SPARK_APP_TAG_LABEL): mutable.Buffer[Pod] =
      selectSparkDrivers().withLabel(sparkAppIdLabel).withLabel(sparkAppTagLabel).list().getItems.asScala
    def getSparkDriverByAppId(appId: String, appIdLabel: String = KUBERNETES_SPARK_APP_ID_LABEL): Option[Pod] =
      Try(selectSparkDrivers().withLabel(appIdLabel, appId).list().getItems.asScala.head).toOption
    def getSparkDriverByAppTag(appTag: String, appTagLabel: String = KUBERNETES_SPARK_APP_TAG_LABEL): Option[Pod] =
      Try(selectSparkDrivers().withLabel(appTagLabel, appTag).list().getItems.asScala.head).toOption
    def getSparkPodsByAppId(appId: String, sparkAppIdLabel: String = KUBERNETES_SPARK_APP_ID_LABEL): mutable.Buffer[Pod] =
      client.pods().inAnyNamespace().withLabel(KUBERNETES_SPARK_APP_ID_LABEL, appId).list().getItems.asScala
  }

  def buildSparkPodDiagnosticsPrettyString(pod: Pod): String = {
    s"${pod.getMetadata.getName}.${pod.getMetadata.getNamespace}:" +
      s"\n\tnode: ${pod.getSpec.getNodeName}" +
      s"\n\thostname: ${pod.getSpec.getHostname}" +
      s"\n\tpodIp: ${pod.getStatus.getPodIP}" +
      s"\n\tstartTime: ${pod.getStatus.getStartTime}" +
      s"\n\tphase: ${pod.getStatus.getPhase}" +
      s"\n\treason: ${pod.getStatus.getReason}" +
      s"\n\tmessage: ${pod.getStatus.getMessage}" +
      s"\n\tlabels: ${printMap(pod.getMetadata.getLabels.asScala)}" +
      s"\n\tcontainers:" +
      s"\n\t\t${
        pod.getSpec.getContainers.asScala.map(container ⇒
          s"${container.getName}:" +
            s"\n\t\t\timage: ${container.getImage}" +
            s"\n\t\t\trequests: ${printMap(container.getResources.getRequests.asScala)}" +
            s"\n\t\t\tlimits: ${printMap(container.getResources.getLimits.asScala)}" +
            s"\n\t\t\tcommand: ${container.getCommand} ${container.getArgs}"
        ).mkString("\n\t\t")
      }" +
      s"\n\tconditions:" +
      s"\n\t\t${pod.getStatus.getConditions.asScala.mkString("\n\t\t")}"
  }

  def printMap(map: mutable.Map[_, _]): String = map.map { case (key, value) ⇒ s"$key=$value"}.mkString(", ")

}

object KubernetesConfig {
  val KUBERNETES_SPARK_APP_ID_LABEL  = "spark-app-selector"
  val KUBERNETES_SPARK_APP_TAG_LABEL = "spark-app-tag"
  val KUBERNETES_SPARK_ROLE_LABEL    = "spark-role"
  val KUBERNETES_SPARK_ROLE_DRIVER   = "driver"
}

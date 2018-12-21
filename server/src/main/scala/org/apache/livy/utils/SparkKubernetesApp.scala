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

import java.io._
import java.{lang, util}
import java.util.UUID
import java.util.concurrent.TimeoutException

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ConfigBuilder => _, _}
import io.fabric8.kubernetes.client._
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, LogWatch, PodResource}
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.livy.server.batch.CreateBatchRequest
import org.apache.hadoop.fs.{CreateFlag, FileContext, Path}
import org.apache.livy.{LivyConf, Logging, Utils}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object SparkKubernetesApp extends Logging {

  private var cacheLogSize      : Int            = _
  private var appLookupTimeout  : FiniteDuration = _
  private var pollInterval      : FiniteDuration = _
  private var logRootPath       : String         = _
  private var recoveryMode      : String         = _
  private var recoveryStateStore: String         = _
  private var logStoreEnabled   : Boolean        = _
  private var logBufferSize     : Int            = _

  private val LOG_FOLDER_PREFIX = "log_"
  private val LOG_FILE_NAME = "log"
  private val LOG_METADATA_FILE_NAME = "_METADATA"


  // KubernetesClient is thread safe. Create once, share it across threads.
  var kubernetesClient: DefaultKubernetesClient = _

  def init(livyConf: LivyConf): Unit = {
    kubernetesClient = KubernetesClientFactory.createKubernetesClient(livyConf)
    cacheLogSize = livyConf.getInt(LivyConf.SPARK_LOGS_SIZE)
    appLookupTimeout = livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT).milliseconds
    pollInterval = livyConf.getTimeAsMs(LivyConf.KUBERNETES_POLL_INTERVAL).milliseconds
    logRootPath = livyConf.get(LivyConf.LOGS_STORE_URL)
    logBufferSize = livyConf.getInt(LivyConf.LOGS_STORE_BUFFER_SIZE)
    recoveryMode = livyConf.get(LivyConf.RECOVERY_MODE)
    recoveryStateStore = livyConf.get(LivyConf.RECOVERY_STATE_STORE)
    logStoreEnabled = livyConf.getBoolean(LivyConf.LOGS_STORE_ENABLED)

    info(s"Initialized SparkKubernetesApp: master=[ ${livyConf.sparkMaster()} ]")
  }

}

class SparkKubernetesApp private[utils](
    appTag: String,
    appIdOption: Option[String],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener],
    livyConf: LivyConf,
    kubernetesClient: => KubernetesClient = SparkKubernetesApp.kubernetesClient) // For unit test.
  extends SparkApp
    with Logging {

  import KubernetesConstants._
  import KubernetesExtensions._
  import KubernetesUtils._
  import SparkApp.State._
  import SparkKubernetesApp._

  private val appIdPromise    : Promise[String] = Promise()
  private val namespacePromise: Promise[String] = Promise()

  private[utils] var state        : SparkApp.State            = SparkApp.State.STARTING
  private        val runningStates: Seq[SparkApp.State.Value] = Seq(STARTING, RUNNING)

  private var kubernetesDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]
  private var kubernetesAppLog     : IndexedSeq[String] = IndexedSeq.empty[String]

  // TODO cache log $logSize, watch and persist full log
  override def log(): IndexedSeq[String] =
    ("stdout: " +: kubernetesAppLog) ++
      ("\nstderr: " +: (process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++ process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String]))) ++
      ("\nKubernetes Diagnostics: " +: kubernetesDiagnostics)

  override def kill(): Unit = synchronized {
    try {
      changeState(SparkApp.State.KILLED)

      kubernetesDiagnostics = ArrayBuffer("Session stopped by user.")
      kubernetesAppMonitorThread.join(pollInterval.toMillis * 2) // wait appMonitoring to finish gracefully
      // TODO wait for logWatchMonitoringThread to finish (and others)
      logMonitorThread.join(pollInterval.toMillis * 2) // wait logMonitoring to finish

    } catch {
      // TODO analyse possible exceptions and handle them
      case _: TimeoutException | _: InterruptedException =>
        warn("Deleting a session while its Kubernetes application is not found.")
        kubernetesAppMonitorThread.interrupt()
      // interrupt other threads of this app
    } finally {
      process.foreach(_.destroy())
      info(s"Attempt to delete namespace [ ${namespacePromise.future.value} ] for app $appTag")
      namespacePromise.future.onComplete(ns ⇒ {
        if (ns.isSuccess) {
          info(s"Spark on Kubernetes app namespace [ ${ns.get} ] was deleted: [ ${kubernetesClient.deleteNamespace(ns.get)} ]")
        } else {
          info(s"Namespace [ $ns ] is not found for app [ $appTag ]")
        }
      })(ExecutionContext.global)
      // TODO check the finalization of app data persistence
    }
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  private def getAppIdFromTag(
      appTag: String,
      pollInterval: Duration,
      deadline: Deadline): Option[String] = {
    val driver = kubernetesClient.getSparkDriverByAppTag(appTag)
    if (driver.isDefined) {
      Option(driver.get.getMetadata.getLabels.get(KUBERNETES_SPARK_APP_ID_LABEL))
    } else {
      if (deadline.isOverdue) {
        kill()
        throw new Exception(s"No Kubernetes application is found with tag $appTag in " +
          appLookupTimeout / 1000 + " seconds. " +
          "Please check your cluster status, it may be very busy.")
      } else {
        Clock.sleep(pollInterval.toMillis)
        getAppIdFromTag(appTag, pollInterval, deadline)
      }
    }
  }

  private def getNamespaceFromTag(
      appTag: String,
      pollInterval: Duration,
      deadline: Deadline): String = {
    val driver = kubernetesClient.getSparkDriverByAppTag(appTag)
    if (driver.isDefined) {
      driver.get.getMetadata.getNamespace
    } else {
      if (deadline.isOverdue) {
        throw new Exception(s"No Kubernetes application is found with tag $appTag in " +
          appLookupTimeout / 1000 + " seconds. " +
          "Please check your cluster status, it is may be very busy.")
      } else {
        Clock.sleep(pollInterval.toMillis)
        getNamespaceFromTag(appTag, pollInterval, deadline)
      }
    }
  }

  private def isRunning: Boolean = runningStates.contains(state)

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

  def findAppId: String = try {
    appIdOption.getOrElse {
      val deadline = appLookupTimeout.fromNow
      getAppIdFromTag(appTag, pollInterval, deadline).get
    }
  } catch {
    case e: Exception =>
      appIdPromise.failure(e)
      throw e
  }

  def findNamespace(appTag: String): String = try {
    val deadline = appLookupTimeout.fromNow
    getNamespaceFromTag(appTag, pollInterval, deadline)
  } catch {
    case e: Exception =>
      namespacePromise.failure(e)
      throw e
  }

  // Exposed for unit test.
  // TODO Instead of spawning a thread for every session, create a centralized thread and
  // batch Kubernetes queries.
  private[utils] val kubernetesAppMonitorThread = Utils.startDaemonThread(s"kubernetesAppMonitorThread-$this") {
    try {
      // If appId is not known, query Kubernetes by appTag to get it.
      val appId = findAppId
      appIdPromise.success(appId)
      namespacePromise.success(findNamespace(appTag))

      Thread.currentThread().setName(s"kubernetesAppMonitorThread-$appId")
      listener.foreach(_.appIdKnown(appId.toString))

      var appInfo = AppInfo()

      while (isRunning) {
        try {

          val sparkPods = kubernetesClient.getSparkPodsByAppTag(appTag)
          val driverPodOption = sparkPods.find(isSparkDriver)

          // Refresh application state
          kubernetesDiagnostics = sparkPods
            .sortBy(_.getMetadata.getName)
            .map(buildSparkPodDiagnosticsPrettyString)
            .flatMap(_.split("\n"))
            .toIndexedSeq
          changeState(mapKubernetesState(Try(driverPodOption.get.getStatus).toOption))

          // Refresh app log cache
          kubernetesAppLog = kubernetesClient.getPodLog(driverPodOption.get, cacheLogSize)
          // Refresh AppInfo links
          val latestAppInfo = {
            val historyServerOption = Option(livyConf.get(LivyConf.HISTORY_SERVER_URL))
            val historyServerInfo = if (historyServerOption.isDefined) Option(s"${historyServerOption.get}/history/$appId/jobs/") else None
            val driverMetadata = Try(driverPodOption.get.getMetadata)
            val sparkUiInfo = if (driverMetadata.isSuccess) {
              val meta = driverMetadata.get
              Option(s"${Option(livyConf.get(LivyConf.SERVER_PROXY_URL)).getOrElse("")}/${meta.getNamespace}/${meta.getName}-svc/jobs/")
            } else {
              None
            }
            AppInfo(sparkUiUrl = sparkUiInfo, historyServerUrl = historyServerInfo)
          }
          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
          Clock.sleep(pollInterval.toMillis)
        } catch {
          // This exception might be thrown during app is starting up. It's transient.
          case e: Throwable => throw e
        }
      }
    } catch {
      case _: InterruptedException =>
        kubernetesDiagnostics = ArrayBuffer("Session stopped by user.")
        changeState(SparkApp.State.KILLED)
      case e: Throwable            =>
        error(s"Error whiling refreshing Kubernetes state: $e")
        kubernetesDiagnostics = ArrayBuffer(e.toString +: e.getStackTrace.map(_.toString): _*)
        changeState(SparkApp.State.FAILED)
    } finally {
      info(s"Finished monitoring app [ $appTag ] in namespace [ ${namespacePromise.future.value} ]")
    }
  }

  private[utils] val logMonitorThread = Utils.startDaemonThread(s"logMonitorThread-$this") {
    if(logStoreEnabled) {
      try {
        Await.ready(appIdPromise.future, appLookupTimeout)
        val appId = appIdPromise.future.value.get.get

        Thread.currentThread().setName(s"logMonitorThread-$appId")

        val logFolderPath = new Path(logRootPath, s"$LOG_FOLDER_PREFIX$appId")
        val logFilePath = new Path(logFolderPath, LOG_FILE_NAME)
        val metadataPath= new Path(logFolderPath, LOG_METADATA_FILE_NAME)

        val logBuffer = new ListBuffer[String]()
        var lastTimestamp:String = null
        var newTimestamp: Option[String] = None

        val fc : FileContext = FileContext.getFileContext(logFolderPath.toUri, livyConf.hadoopConf)

        def writeMetadata(): Unit = {
          newTimestamp = Try{logBuffer.last.split(" ")(0)}.toOption
          if (newTimestamp.isDefined && newTimestamp.get != lastTimestamp) {
            lastTimestamp = newTimestamp.get
            writeLineToFile(fc, metadataPath, lastTimestamp)
          }
        }

        val isRecovery = fc.util().exists(logFilePath) && recoveryMode.equals("recovery") && recoveryStateStore.equals("filesystem")
        val writerMode = if (isRecovery) CreateFlag.APPEND else CreateFlag.OVERWRITE

        val driver = kubernetesClient.getSparkDriverByAppTag(appTag)
        val pod = kubernetesClient.getPodResource(driver.get).usingTimestamps()

        var out = new BufferedWriter(new OutputStreamWriter(fc.create(logFilePath, util.EnumSet.of(CreateFlag.CREATE, writerMode), CreateOpts.createParent())))

        info(s"Attempt to write logs for app [ $appTag ] in namespace [ ${namespacePromise.future.value} ] to path [ $logFilePath ], recovery mode: [ $isRecovery ] ")

        var watchLog : LogWatch = null
        try {
          watchLog = if (isRecovery) {
            val previousTimestamp = readLineFromFile(fc, metadataPath)
            pod.sinceTime(previousTimestamp).watchLog()
          } else {
            pod.watchLog()
          }
          val logReader = new BufferedReader(new InputStreamReader(watchLog.getOutput))

          while (isRunning) {
            if (logBuffer.length < logBufferSize) {
              if (logReader.ready()) {
                val line = logReader.readLine()
                logBuffer.append(line)
              }else{
                Clock.sleep(pollInterval.toMillis)
              }
            } else {
              writeMetadata()
              cleanBufferCloseWriter(logBuffer, out)
              //reopen out writer
              out = new BufferedWriter(new OutputStreamWriter(fc.create(logFilePath, util.EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))))
            }
          }
        }catch{
          case e: Throwable => error(s"Error while logs consuming: " + ArrayBuffer(e.toString +: e.getStackTrace.map(_.toString): _*).mkString("\n"))
        }finally {
          if (watchLog != null) {
            writeMetadata()
            cleanBufferCloseWriter(logBuffer, out)
            watchLog.close()
          }
        }
      } catch {
        case e: Throwable => error(s"Error during logs monitoring thread execution: " + ArrayBuffer(e.toString +: e.getStackTrace.map(_.toString): _*).mkString("\n"))
      }finally {
        info(s"Finished log monitoring app [ $appTag ] in namespace [ ${namespacePromise.future.value} ]")
      }
    }
  }

  def cleanBufferCloseWriter(buffer: ListBuffer[String], out: BufferedWriter): Unit ={
    buffer.foreach(x => {
      out.write(x)
      out.newLine()
    })
    //because flush has no influence we should close writer and reopen it
    out.close()
    buffer.clear()
  }

  def writeLineToFile(fc: FileContext, path: Path, content: String): Unit = {
    val createFlags = util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
    val metadataOut = fc.create(path, createFlags)
    metadataOut.write(content.getBytes)
    metadataOut.flush()
    metadataOut.close()
  }

  def readLineFromFile(fc: FileContext, path: Path): String = {
    val metadataIn = new BufferedReader(new InputStreamReader(fc.open(path)))
    val content = metadataIn.readLine()
    metadataIn.close()
    content
  }

  def buildSparkPodDiagnosticsPrettyString(pod: Pod): String = {
    def printMap(map: mutable.Map[_, _]): String = map.map {
      case (key, value) ⇒ s"$key=$value"
    }.mkString(", ")

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

}

object KubernetesExtensions {

  import KubernetesConstants._

  implicit class KubernetesClientExtensions(client: KubernetesClient) {
    def selectSparkDrivers(sparkRoleLabel: String = KUBERNETES_SPARK_ROLE_LABEL, sparkRoleDriver: String = KUBERNETES_SPARK_ROLE_DRIVER): FilterWatchListDeletable[Pod, PodList, lang.Boolean, Watch, Watcher[Pod]] =
      client.pods.inAnyNamespace.withLabel(sparkRoleLabel, sparkRoleDriver)

    def getSparkDriverByAppTag(appTag: String, appTagLabel: String = KUBERNETES_SPARK_APP_TAG_LABEL): Option[Pod] =
      Try(selectSparkDrivers().withLabel(appTagLabel, appTag).list.getItems.asScala.head).toOption

    def getSparkPodsByAppTag(appTag: String, sparkAppTagLabel: String = KUBERNETES_SPARK_APP_TAG_LABEL): mutable.Buffer[Pod] =
      client.pods.inAnyNamespace.withLabel(sparkAppTagLabel, appTag).list().getItems.asScala

    def getNamespacesWithPrefix(prefix: String): mutable.Buffer[Namespace] =
      client.namespaces().list().getItems.asScala.filter(_.getMetadata.getName.startsWith(prefix))

    def createNamespace(name: String): Namespace = client.namespaces.create(buildNamespace(name))

    def containsNamespace(name: String): Boolean = client.namespaces.list.getItems.asScala.map(_.getMetadata.getName).contains(name)

    def deleteNamespace(name: String): Boolean = client.namespaces.delete(buildNamespace(name))

    def buildNamespace(name: String): Namespace = new NamespaceBuilder().withNewMetadata.withName(name).endMetadata.build()

    def createOrReplaceImagePullSecret(namespace: String, name: String, content: String): Secret = {
      val secret = new SecretBuilder()
        .withNewMetadata().withName(name).withNamespace(namespace).endMetadata
        .withType(KUBERNETES_IMAGE_PULL_SECRET_TYPE)
        .addToData(KUBERNETES_IMAGE_PULL_SECRET_DATA_KEY, content)
        .build()
      client.secrets.delete(secret)
      client.secrets.create(secret)
    }

    def getPodLog(pod: Pod, cacheLogSize: Int): IndexedSeq[String] = try {
      getPodResource(pod).tailingLines(cacheLogSize).getLog.split("\n").toIndexedSeq
    } catch {
      case e: Throwable ⇒
        ArrayBuffer(e.toString +: e.getStackTrace.map(_.toString): _*)
    }

    def getPodResource(pod:Pod): PodResource[Pod, DoneablePod] = {
      val name = pod.getMetadata.getName
      val namespace = pod.getMetadata.getNamespace
      client.pods.inNamespace(namespace).withName(name)
    }
  }
}

object KubernetesConstants {
  val KUBERNETES_SPARK_APP_ID_LABEL  = "spark-app-selector"
  val KUBERNETES_SPARK_APP_TAG_LABEL = "spark-app-tag"
  val KUBERNETES_SPARK_ROLE_LABEL    = "spark-role"
  val KUBERNETES_SPARK_ROLE_DRIVER   = "driver"

  val KUBERNETES_IMAGE_PULL_SECRET_TYPE     = "kubernetes.io/dockerconfigjson"
  val KUBERNETES_IMAGE_PULL_SECRET_DATA_KEY = ".dockerconfigjson"
}

object KubernetesUtils extends Logging {

  import KubernetesConstants._

  def formatAppId(appId: String): String = {
    val formatted = s"stub.$appId".split("\\.").last.toLowerCase().replaceAll("[^0-9a-z]", "")
    val shortened = if (formatted.length > 32) formatted.substring(0, 32) else formatted
    s"$shortened-${System.currentTimeMillis()}"
  }

  def getAppId(appId: Option[String], appName: Option[String], className: Option[String]): String = {
    if (appId.isDefined) {
      appId.get
    } else if (appName.isDefined) {
      formatAppId(appName.get)
    } else if (className.isDefined) {
      formatAppId(className.get)
    } else {
      s"spark-${UUID.randomUUID().toString.replaceAll("-", "")}"
    }
  }

  def generateKubernetesNamespace(appTag: String, prefix: String, sparkConf: Map[String, String]): String = {
    s"$prefix${sparkConf.getOrElse("spark.kubernetes.namespace", appTag)}"
  }

  def encodeImagePullSecretContent(registry: String, user: String, password: String): String = {
    val auth = BaseEncoding.base64.encode(s"$user:$password".getBytes(Charsets.UTF_8))
    BaseEncoding.base64.encode(s"""{"auths":{"$registry":{"auth":"$auth"}}}""".getBytes(Charsets.UTF_8))
  }

  def prepareKubernetesNamespace(livyConf: LivyConf, namespace: String): Unit = {
    import KubernetesExtensions._
    import SparkKubernetesApp.kubernetesClient

    if (!kubernetesClient.containsNamespace(namespace)) kubernetesClient.createNamespace(namespace)
    val secretName = livyConf.get(LivyConf.KUBERNETES_IMAGE_PULL_SECRET_NAME).toOption
    val registry = livyConf.get(LivyConf.KUBERNETES_IMAGE_PULL_SECRET_REGISTRY).toOption
    val user = livyConf.get(LivyConf.KUBERNETES_IMAGE_PULL_SECRET_USER).toOption
    val password = livyConf.get(LivyConf.KUBERNETES_IMAGE_PULL_SECRET_PASSWORD).toOption
    require(Seq(secretName, registry, user, password).forall(_.isDefined) || Seq(secretName, registry, user, password).forall(_.isEmpty),
      "ImagePullSecret config options should either be set all or none: livy.server.kubernetes.imagePullSecret.[name, registry, user, password]")
    val secretContent = encodeImagePullSecretContent(registry.get, user.get, password.get)
    kubernetesClient.inAnyNamespace.createOrReplaceImagePullSecret(namespace, secretName.get, secretContent)
  }

  def prepareKubernetesSpecificConf(namespace: String, request: CreateBatchRequest): Map[String, String] = Map(
    "spark.app.id" → getAppId(Try(request.conf("spark.app.id")).toOption, request.name, request.className),
    "spark.kubernetes.namespace" → namespace
  )

  def parseCreationTime(resource: HasMetadata): DateTime =
    org.joda.time.DateTime.parse(resource.getMetadata.getCreationTimestamp).withZone(DateTimeZone.UTC)

  def isExpired(resource: HasMetadata, ttl: FiniteDuration): Boolean =
    parseCreationTime(resource).plus(ttl.toMillis).isBefore(DateTime.now(DateTimeZone.UTC))

  def isSparkDriver(pod: Pod): Boolean = {
    pod.getMetadata.getLabels.containsKey(KUBERNETES_SPARK_ROLE_LABEL) &&
      pod.getMetadata.getLabels.get(KUBERNETES_SPARK_ROLE_LABEL) == KUBERNETES_SPARK_ROLE_DRIVER
  }

  def isSparkDriverExpired(driver: Pod, ttl: FiniteDuration): Boolean =
    isExpired(driver, ttl) && isSparkDriverFinished(driver.getStatus.getPhase)

  def isSparkDriverFinished(phase: String): Boolean =
    Try(Seq("succeeded", "failed").contains(phase.toLowerCase) || phase.toLowerCase.contains("backoff")).getOrElse(false)

  implicit class OptionString(val string: String) extends AnyVal {
    def toOption: Option[String] = if (string == null || string.isEmpty) None else Option(string)
  }

}

object KubernetesClientFactory {

  import KubernetesUtils.OptionString

  def createKubernetesClient(livyConf: LivyConf): DefaultKubernetesClient = {
    val masterUrl = livyConf.get(LivyConf.KUBERNETES_MASTER_URL).toOption.getOrElse("https://kubernetes.default.svc")

    val oauthTokenFile = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_FILE).toOption
    val oauthTokenValue = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_VALUE).toOption
    require(oauthTokenFile.isEmpty || oauthTokenValue.isEmpty,
      s"Cannot specify OAuth token through both a file $oauthTokenFile and a value $oauthTokenValue.")

    val caCertFile = livyConf.get(LivyConf.KUBERNETES_CA_CERT_FILE).toOption
    val clientKeyFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_KEY_FILE).toOption
    val clientCertFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_CERT_FILE).toOption

    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(masterUrl)
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }
      .withOption(oauthTokenFile) {
        (filePath, configBuilder) => configBuilder.withOauthToken(Files.toString(new File(filePath), Charsets.UTF_8))
      }
      .withOption(caCertFile) {
        (file, configBuilder) => configBuilder.withCaCertFile(file)
      }
      .withOption(clientKeyFile) {
        (file, configBuilder) => configBuilder.withClientKeyFile(file)
      }
      .withOption(clientCertFile) {
        (file, configBuilder) => configBuilder.withClientCertFile(file)
      }
      .build()
    new DefaultKubernetesClient(config)
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder) extends AnyVal {
    def withOption[T]
    (option: Option[T])
      (configurator: (T, ConfigBuilder) => ConfigBuilder): ConfigBuilder = {
      option.map {
        opt => configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }

}

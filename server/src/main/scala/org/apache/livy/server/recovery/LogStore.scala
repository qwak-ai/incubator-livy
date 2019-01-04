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
package org.apache.livy.server.recovery

import org.apache.livy.sessions.SessionManager._
import org.apache.livy.{LivyConf, Logging}

abstract class LogStore(livyConf: LivyConf) {
  def save(data: Seq[String], key: String): Unit

  def get(key: String): IndexedSeq[String]

  def remove(key: String): Unit
}

/**
  * Factory to create the store chosen in LivyConf.
  */
object LogStore extends Logging {
  private[this] var logStore: Option[LogStore] = None

  def init(livyConf: LivyConf): Unit = synchronized {
    if (logStore.isEmpty) {
      val fileLogStoreClassTag = pickLogStore(livyConf)
      logStore = Option(fileLogStoreClassTag.getDeclaredConstructor(classOf[LivyConf])
        .newInstance(livyConf).asInstanceOf[LogStore])
      info(s"Using ${logStore.get.getClass.getSimpleName} for logs recovery.")
    }
  }

  def cleanup(): Unit = synchronized {
    logStore = None
  }

  def get: LogStore = {
    assert(logStore.isDefined, "LogStore hasn't been initialized.")
    logStore.get
  }

  private[recovery] def pickLogStore(livyConf: LivyConf): Class[_] = {
    livyConf.get(LivyConf.RECOVERY_MODE) match {
      case SESSION_RECOVERY_MODE_OFF      => classOf[BlackholeLogStore]
      case SESSION_RECOVERY_MODE_RECOVERY =>
        livyConf.get(LivyConf.RECOVERY_LOG_STORE) match {
          case "off"        => classOf[BlackholeLogStore]
          case "filesystem" => classOf[FileSystemLogStore]
          case ls           => throw new IllegalArgumentException(s"Unsupported log store $ls")
        }
      case rm                             => throw new IllegalArgumentException(s"Unsupported recovery mode $rm")
    }
  }
}

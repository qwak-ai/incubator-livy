package org.apache.livy.server.recovery

import java.io._
import java.net.URI
import java.util

import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{CreateFlag, FileAlreadyExistsException, FileContext, Path}
import org.apache.livy.Utils._
import org.apache.livy.{LivyConf, Logging}

class FileSystemLogStore(
    livyConf: LivyConf,
    mockFileContext: Option[FileContext])
  extends LogStore(livyConf) with Logging {

  private final val LOG_FILE_NAME = "log"

  // Constructor defined for LogStore factory to new this class using reflection.
  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val fsUri = {
    val fsPath = livyConf.get(LivyConf.RECOVERY_LOG_STORE_URL)
    require(!fsPath.isEmpty, s"Please config ${LivyConf.RECOVERY_LOG_STORE_URL.key}.")
    new URI(fsPath)
  }

  private val fileContext: FileContext = mockFileContext.getOrElse {
    FileContext.getFileContext(fsUri, livyConf.hadoopConf)
  }

  {
    // Only Livy user should have access to log files.
    fileContext.setUMask(new FsPermission("077"))

    // Create log store dir if it doesn't exist.
    val logStorePath = absPath(".")
    try {
      fileContext.mkdir(logStorePath, FsPermission.getDirDefault, true)
    } catch {
      case _: FileAlreadyExistsException =>
        if (!fileContext.getFileStatus(logStorePath).isDirectory) {
          throw new IOException(s"$logStorePath is not a directory.")
        }
    }

    // Check permission of log store dir.
    val fileStatus = fileContext.getFileStatus(absPath("."))
    require(fileStatus.getPermission.getUserAction == FsAction.ALL,
      s"Livy doesn't have permission to access log store: $fsUri.")
    if (fileStatus.getPermission.getGroupAction != FsAction.NONE) {
      warn(s"Group users have permission to access log store: $fsUri. This is insecure.")
    }
    if (fileStatus.getPermission.getOtherAction != FsAction.NONE) {
      warn(s"Other users have permission to access log store: $fsUri. This is in secure.")
    }
  }

  override def save(data: Seq[String], key: String): Unit = {
    if (data != null && data.nonEmpty) {
      val createFlags = util.EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
      usingResource(new BufferedWriter(new OutputStreamWriter(fileContext.create(
        logFilePath(key), createFlags, CreateOpts.createParent()))
      )) {
        writer =>
        data.foreach(line => {
          writer.write(line)
          writer.newLine()
        })
      }
    }
  }

  override def get(key: String): IndexedSeq[String] = {
    usingResource(fileContext.open(logFilePath(key))) {
      stream => scala.io.Source.fromInputStream(stream).getLines().toIndexedSeq
    }
  }

  override def remove(key: String): Unit = {
    fileContext.delete(absPath(key), true)
  }

  private def absPath(relativePath: String): Path = new Path(fsUri.getPath, relativePath)

  private def logFilePath(key: String): Path = absPath(s"$key/$LOG_FILE_NAME")

}

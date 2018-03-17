package util

import java.io.{IOException, File}

import org.apache.commons.io.FileUtils

/**
 * Set up temporary directories, to be deleted automatically at shutdown. If the directories
 * exist at creation time they will be cleaned up (deleted) first.
 */
private[util] class TemporaryDirectories {
  val tempRootPath = java.io.File.separator + "tmp" + java.io.File.separator + "SSWK"

  val checkpointPath = tempRootPath + File.separator + "checkpoints"

  private val rootDir = new File(tempRootPath)

  // delete in advance in case last cleanup didn't
  deleteRecursively(rootDir)
  rootDir.mkdir

  val zkSnapshotPath = tempRootPath + File.separator + "zookeeper-snapshot"
  val zkSnapshotDir = new File(zkSnapshotPath)
  zkSnapshotDir.mkdir()

  val zkLogDirPath = tempRootPath + File.separator + "zookeeper-logs"
  val zkLogDir = new File(zkLogDirPath)
  zkLogDir.mkdir()

  val kafkaLogDirPath = tempRootPath + File.separator + "kafka-logs"
  val kafkaLogDir = new File(kafkaLogDirPath)
  kafkaLogDir.mkdir()


  Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
    def run {
      try {
        deleteRecursively(rootDir)
      }
      catch {
        case e: Exception => {
        }
      }
    }
  }))


  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}

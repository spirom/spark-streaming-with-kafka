package util

import java.io.File

object Checkpoints {
  def create : String = {
    val checkpointPath = java.io.File.separator + "tmp" + java.io.File.separator + "SSWK" +
      File.separator + "checkpoints"
    val checkpointDir = new File(checkpointPath)
    deleteRecursively(checkpointDir)
    checkpointDir.mkdir
    checkpointDir.deleteOnExit()
    checkpointPath
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}

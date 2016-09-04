package util

import org.apache.spark.streaming.StreamingContext


object Monitoring {
  def getMonitorThread(ssc: StreamingContext) : Thread = {
    val t = new Thread("Streaming Termination Monitor") {
      override def run() {
        try {
          ssc.awaitTermination()
        } catch {
          case e: Exception => {
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
          }
        }
        println("*** streaming terminated")
      }
    }
    t.start()
    t
  }
}

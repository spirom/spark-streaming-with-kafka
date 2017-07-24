package util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

//
// This is a general tool for analyzing an RDD of ConsumerRecord, such as is normally produced by a Kafka stream.
// The goal is to see how subscribed Kafka topics and their Kafka partitions map to partitions in the RDD that is
// emitted by the Spark stream. The code is a little convoluted because of its contradictory goals:
//    1) avoid collecting the RDD tot he driver node (thus having to serialize ConsumerRecord
//    2) print the partition information sequentially to keep the output from being jumbled
//
// It may be fun to rewrite it so that a data structure containing this infrastructure is produced in parallel
// and then collected and printed sequentially.
//
object PartitionMapAnalyzer {

  def analyze[K,V](r: RDD[ConsumerRecord[K,V]],
                   dumpRecords: Boolean = false) : Unit =
  {
    if (r.count() > 0) {
      println("*** " + r.getNumPartitions + " partitions")

      val partitions = r.glom().zipWithIndex()

      // this loop will be sequential; each iteration analyzes one partition
      (0l to partitions.count() - 1).foreach(n => analyzeOnePartition(partitions, n, dumpRecords))

    } else {
      println("*** RDD is empty")
    }
  }

  private def analyzeOnePartition[K,V](partitions: RDD[(Array[ConsumerRecord[K, V]], Long)],
                                       which: Long,
                                       dumpRecords: Boolean) : Unit =
  {
    partitions.foreach({
      case (data: Array[ConsumerRecord[K, V]], index: Long) => {
        if (index == which) {
          println(s"*** partition $index has ${data.length} records")
          data.groupBy(cr => (cr.topic(), cr.partition())).foreach({
            case (k: (String, Int), v: Array[ConsumerRecord[K, V]]) =>
              println(s"*** rdd partition = $index, topic = ${k._1}, topic partition = ${k._2}, record count = ${v.length}.")
          })
          if (dumpRecords) data.foreach(cr => println(s"RDD partition $index record $cr"))
        }
      }
    })
  }
}

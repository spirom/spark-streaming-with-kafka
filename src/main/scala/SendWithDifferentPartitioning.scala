import java.util.{Arrays, Properties}

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, SimpleKafkaClient, SparkKafkaSink}

/**
  * This example is very similar to SimpleStreaming, except that the data is sent
  * from an RDD with 5 partitions to a Kafka topic with 6 partitions. WThe KafkaStream consuming
  * the topic produces RDDs with size partitions. This is because the data is repartitioned when sent,
  * as we continue use the KafkaProducer constructor overload that doesn't allow us to specify
  * the destination partition.
  */
object SendWithDifferentPartitioning {

  /**
    * Publish some data to a topic. Encapsulated here to ensure serializability.
    * @param max
    * @param sc
    * @param topic
    * @param config
    */
  def send(max: Int, sc: SparkContext, topic: String, config: Properties): Unit = {

    // put some data in an RDD and publish to Kafka
    val numbers = 1 to max
    val numbersRDD = sc.parallelize(numbers, 5)

    val kafkaSink = sc.broadcast(SparkKafkaSink(config))

    println("*** producing data")

    numbersRDD.foreach { n =>
      // NOTE:
      //     1) the keys and values are strings, which is important when receiving them
      //     2) We don't specify which Kafka partition to send to, so a hash of the key
      //        is used to determine this
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }
  }

  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 6)



    val conf = new SparkConf().setAppName("SendWithDifferentPartitioning").setMaster("local[7]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val max = 1000

    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
    })

    ssc.start()

    println("*** started termination monitor")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThread = new Thread("Streaming Termination Controller") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        send(max, sc, topic, client.basicStringStringProducer)
        Thread.sleep(5000)
        println("*** requesting streaming termination")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      }
    }
    producerThread.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    // stop Spark
    sc.stop()

    // stop Kafka
    kafkaServer.stop()

    println("*** done")
  }
}
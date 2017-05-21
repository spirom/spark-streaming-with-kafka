import java.util.Properties
import java.util.Arrays

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, SimpleKafkaClient, SparkKafkaSink}
import java.util

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * The most basic streaming example: starts a Kafka server, creates a topic, creates a stream
  * to process that topic, and publishes some data using the SparkKafkaSink.
  *
  * Notice there's quite a lot of waiting. It takes some time for streaming to get going,
  * and data published too early tends to be missed by the stream. (No doubt, this is partly
  * because this example uses the simplest method to create the stream, and thus doesn't
  * get an opportunity to set auto.offset.reset to "earliest".
  *
  * Also, data that is published takes some time to propagate to the stream.
  * This seems inevitable, and is almost guaranteed to be slower
  * in a self-contained example like this.
  */
object SimpleStreaming {

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
    val numbersRDD = sc.parallelize(numbers, 4)

    val kafkaSink = sc.broadcast(SparkKafkaSink(config))

    println("*** producing data")

    numbersRDD.foreach { n =>
      // notice the keys and values are strings, which is important when receiving them
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }
  }

  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)



    val conf = new SparkConf().setAppName("SimpleStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // this many messages
    val max = 1000

    // only subscribing to one topic and all four of its partitions
    val topicMap =
      Map[String, Int](topic -> 4)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer.getKafkaConnect,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MyGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Create the stream. Group name doesn't matter as there won't be other subscribers.
    // Notice that the default is to assume the topic is receiving String keys and values,
    // which is what is being sent.
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

    //, "MyGroup", topicMap)

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
import java.util.{Arrays, Properties}

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, SimpleKafkaClient, SparkKafkaSink}

/**
  * Partitions can be added to a Kafka topic dynamically. This example shows that an existing stream
  * will not see the data published to the new partitions, and only when the existing streaming context is terminated
  * and a new stream is started from a new context will that data be delivered.
  *
  * The topic is created with three partitions, and so each RDD the stream produces has three partitions as well,
  * even after two more partitions are added to the topic. When a new stream is subsequently created, the RDDs produced
  * have five partitions, but only two of them contain data, as all the data has been drained from the initial three
  * partitions of the topic, by the first stream.
  */
object AddPartitionsWhileStreaming {

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
    kafkaServer.createTopic(topic, 3)



    val conf = new SparkConf().setAppName("AddPartitionsWhileStreaming").setMaster("local[7]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val max = 500

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
      println("[1] *** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("[1] *** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("[1] *** partition size = " + a.size))
      }
    })

    ssc.start()

    println("*** started streaming context")

    // streams seem to need some time to get going
    Thread.sleep(5000)


    val client = new SimpleKafkaClient(kafkaServer)

    send(max, sc, topic, client.basicStringStringProducer)
    Thread.sleep(5000)

    println("*** adding partitions to topic")

    kafkaServer.addPartitions(topic, 5)

    Thread.sleep(5000)

    send(max, sc, topic, client.basicStringStringProducer)

    Thread.sleep(5000)

    println("*** stop first streaming context")
    ssc.stop(stopSparkContext = false)
    try {
      ssc.awaitTermination()
      println("*** streaming terminated for the first time")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread (first context)")
      }
    }

    println("*** create second streaming context")
    val ssc2 = new StreamingContext(sc, Seconds(1))

    println("*** create a second stream from the second streaming context")
    val kafkaStream2 =
      KafkaUtils.createDirectStream(
        ssc2,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    kafkaStream2.foreachRDD(r => {
      println("[2] *** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("[2] *** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("[2] *** partition size = " + a.size))
      }
    })

    println("*** start second streaming context")
    ssc2.start()

    Thread.sleep(5000)

    println("*** requesting streaming termination")
    ssc2.stop(stopSparkContext = false, stopGracefully = true)


    try {
      ssc2.awaitTermination()
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
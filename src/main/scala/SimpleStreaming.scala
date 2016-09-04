import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DirectKafkaClient, EmbeddedKafkaServer, Monitoring, SparkKafkaSink}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * The most basic streaming example: starts a Kafka server, creates a topic, creates a stream
  * to process that topic, and publishes some data using the SparkKafkaSink.
  *
  * Notice there's quite a lot of waiting. It takes some time for streaming to get going,
  * and data published too early tends to be missed by the stream. (No doubt,t his is partly
  * because this example sues the simplest method to create the stream, and thus doesn't
  * get an opportunity to set auto.offset.reset to "earliest".
  *
  * Also, data that is published takes some time to propagate to the stream.
  * This seems inevitable, and is almost guaranteed to be slower
  * in a self-contained example like this.
  */
object SimpleStreaming {
  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    val client = new DirectKafkaClient(kafkaServer.getKafkaConnect)


    val conf = new SparkConf().setAppName("SimpleStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val max = 10

    // only subscribing to one topic and all four partitions
    val topicMap =
      Map[String, Int](topic -> 4)
    // Create the stream. Group doesn't matter as there won't be other subscribers.
    // Notice that the default is to assume the topic is receiving String keys and values.
    val kafkaStream =
      KafkaUtils.createStream(ssc, kafkaServer.getZkConnect, "MyGroup", topicMap)

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
    })

    ssc.start()

    val monitorThread = Monitoring.getMonitorThread(ssc)

    println("*** started termination monitor")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    // put some data in an RDD and publish to Kafka
    val numbers = 1 to max
    val numbersRDD = sc.parallelize(numbers, 4)

    val kafkaSink = sc.broadcast(SparkKafkaSink(client.getBasicStringStringProducer(kafkaServer)))

    println("*** producing data")

    numbersRDD.foreach { n =>
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }

    // wait for the data to propagate --
    Thread.sleep(5000)

    // stop streaming
    ssc.stop(true)

    // make sure any messages get printed
    monitorThread.join()

    // stop Spark
    sc.stop()

    // stop Kafka
    kafkaServer.stop()

    println("*** done")
  }
}
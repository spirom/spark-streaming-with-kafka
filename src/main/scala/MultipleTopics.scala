import java.util.{Arrays, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, PartitionMapAnalyzer, SimpleKafkaClient}

/**
  * A single stream subscribing to the two topics receives data from both of them.
  * The partitioning behavior here is quite interesting, as the topics have three and six partitions respectively,
  * each RDD has nine partitions, and each RDD partition receives data from exactly one partition of one topic.
  *
  * Partitioning is analyzed using the PartitionMapAnalyzer.
  */
object MultipleTopics {

  def main (args: Array[String]) {

    val topic1 = "foo"
    val topic2 = "bar"

    // topics are partitioned differently
    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic1, 3)
    kafkaServer.createTopic(topic2, 6)

    val conf = new SparkConf().setAppName("MultipleTopics").setMaster("local[10]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // this many messages
    val max = 100

    // Create the stream.
    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic1, topic2),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())

      PartitionMapAnalyzer.analyze(r)

    })

    ssc.start()

    println("*** started streaming context")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThreadTopic1 = new Thread("Producer thread 1") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, String](client.basicStringStringProducer)

        numbers.foreach { n =>
          // NOTE:
          //     1) the keys and values are strings, which is important when receiving them
          //     2) We don't specify which Kafka partition to send to, so a hash of the key
          //        is used to determine this
          producer.send(new ProducerRecord(topic1, "key_1_" + n, "string_1_" + n))
        }

      }
    }

    val producerThreadTopic2 = new Thread("Producer thread 2; controlling termination") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, String](client.basicStringStringProducer)

        numbers.foreach { n =>
          // NOTE:
          //     1) the keys and values are strings, which is important when receiving them
          //     2) We don't specify which Kafka partition to send to, so a hash of the key
          //        is used to determine this
          producer.send(new ProducerRecord(topic2, "key_2_" + n, "string_2_" + n))
        }
        Thread.sleep(10000)
        println("*** requesting streaming termination")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      }
    }

    producerThreadTopic1.start()
    producerThreadTopic2.start()

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
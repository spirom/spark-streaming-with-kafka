
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{DirectKafkaClient, EmbeddedKafkaServer, SparkKafkaSink}

object Basic {
  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic)

    val client = new DirectKafkaClient(kafkaServer.getKafkaConnect)


    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val max = 10

    // put some data in an RDD
    val numbers = 1 to max
    val numbersRDD = sc.parallelize(numbers, 4)

    val kafkaSink = sc.broadcast(SparkKafkaSink(client.getBasicStringStringProducer(kafkaServer)))

    numbersRDD.foreach { n =>
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }

    val consumerConfig = client.getBasicStrignStringConsumer(kafkaServer)

    client.consumeAndPrint(consumerConfig, topic, max)

    kafkaServer.stop()

    sc.stop()

    println("*** done")
  }
}
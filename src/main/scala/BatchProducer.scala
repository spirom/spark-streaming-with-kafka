
import org.apache.spark.{SparkConf, SparkContext}
import util.{DirectKafkaClient, EmbeddedKafkaServer, SparkKafkaSink}

/**
  * This example sues Spark but doesn't use streaming. It starts a Kafka server, creates a
  * topic, and uses the SparkKafkaSink utility to publish the RDD to the topic in parallel.
  * It then uses the DirectKafkaClient utility to examine the topic. The goal here is
  * demonstrate SparkKafkaSink, which will be used in some of the streaming examples so
  * that a stream consuming data from on Kafka topic can process it and publish it to another
  * Kafka topic.
  */
object BatchProducer {
  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic)

    val client = new DirectKafkaClient(kafkaServer.getKafkaConnect)


    val conf = new SparkConf().setAppName("BatchProducer").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val max = 10

    // put some data in an RDD
    val numbers = 1 to max
    val numbersRDD = sc.parallelize(numbers, 4)

    // make sure SparkKafkaSink is available to each partition by broadcasting it
    val kafkaSink = sc.broadcast(SparkKafkaSink(client.getBasicStringStringProducer(kafkaServer)))

    // publish the data
    numbersRDD.foreach { n =>
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }

    // read the data back using the DirectKafkaClient utility and print it
    val consumerConfig = client.getBasicStrignStringConsumer(kafkaServer)
    client.consumeAndPrint(consumerConfig, topic, max)

    kafkaServer.stop()

    sc.stop()

    println("*** done")
  }
}
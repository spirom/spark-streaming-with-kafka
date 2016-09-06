
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import util.{SimpleKafkaClient, EmbeddedKafkaServer}


/**
  * This example doesn't use Spark at all: it simply shows how to use the utility
  * classes to run an embedded Kafka instance, create a topic, use a producer to
  * publish some records, and a consumer to read them back. They are used in various
  * places across the remaining examples either for setup or to validate the results
  * of executing the Spark code.
  */
object UtilDemo {
  def main (args: Array[String]): Unit = {

    val topic = "foo"

    // first star an embedded server

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()

    // create a topic

    kafkaServer.createTopic(topic, 4)

    val client = new SimpleKafkaClient(kafkaServer)

    // configure a producer and send some records

    val producerConfig = client.getBasicStringStringProducer(kafkaServer)

    val producer = new KafkaProducer[String, String](producerConfig)

    val max = 20

    for (i <- 1 to max) {
      val key = "KEY_" + i
      val value = "VALUE_" + i
      producer.send(new ProducerRecord(topic, key, value))
    }

    val consumerConfig = client.getBasicStrignStringConsumer(kafkaServer)

    Thread.sleep(5000)

    client.consumeAndPrint(consumerConfig, topic, max)

    // shut down the Kafka server

    kafkaServer.stop()

    println("*** done")

  }
}

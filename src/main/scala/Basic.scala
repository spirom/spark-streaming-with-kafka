import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}

object Basic {
  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic)

    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))



    // put some data in an RDD
    val numbers = 1 to 10
    val numbersRDD = sc.parallelize(numbers, 4)

    val producerConfig: Properties = new Properties
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getKafkaConnect())

    val kafkaSink = sc.broadcast(KafkaSink(producerConfig))

    numbersRDD.foreach { message =>
      kafkaSink.value.send("foo", "string_" + message)
    }

    kafkaServer.stop()

    sc.stop()

    println("*** done")
  }
}
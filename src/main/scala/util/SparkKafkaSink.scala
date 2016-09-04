package util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class SparkKafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }
}

object SparkKafkaSink {
  def apply(config: Properties): SparkKafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new SparkKafkaSink(f)
  }
}
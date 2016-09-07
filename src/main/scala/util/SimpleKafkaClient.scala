package util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


/**
  * Simple utilities for connecting directly to Kafka.
  */
class SimpleKafkaClient(server: EmbeddedKafkaServer) {

  def getBasicStringStringProducer(server: EmbeddedKafkaServer) : Properties = {
    val config: Properties = new Properties
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    //config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner")
    config
  }

  def getBasicStrignStringConsumer(server: EmbeddedKafkaServer) : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroup")
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest")
    consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")

    consumerConfig
  }
}

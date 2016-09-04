package util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConversions._


class DirectKafkaClient(connection: String) {

  def consumeAndPrint(config: Properties, topic: String, max: Int): Unit = {
    // configure a consumer


    val consumer = new KafkaConsumer[String, String](config);

    // need to subscribe to the topic

    consumer.subscribe(Seq(topic))

    // and read the records back -- just keep polling until we have read
    // all of them (poll each 100 msec) as the Kafka server may not make
    // them available immediately

    var count = 0;

    while (count < max) {
      println("*** Polling ")

      val records: ConsumerRecords[String, String] =
        consumer.poll(100)
      println(s"*** received ${records.count} messages")
      count = count + records.count

      // must specify the topic as we could have subscribed to more than one
      records.records(topic).foreach(rec => println(rec.key() + ":" + rec.value()))
    }

    println("*** got the expected number of messages")
  }

  def getBasicStringStringProducer(server: EmbeddedKafkaServer) : Properties = {
    val config: Properties = new Properties
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection)
    config
  }

  def getBasicStrignStringConsumer(server: EmbeddedKafkaServer) : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroup")
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig
  }
}

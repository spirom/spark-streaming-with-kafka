package util

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


/**
  * Simple utilities for connecting directly to Kafka.
  */
class SimpleKafkaClient(server: EmbeddedKafkaServer) {

  def send(topic: String, pairs: Seq[(String, String)]) : Unit = {
    val producer = new KafkaProducer[String, String](basicStringStringProducer)
    pairs.foreach(pair => {
      producer send(new ProducerRecord(topic, pair._1, pair._2))
    })
    producer.close()
  }

  /**
    * Read and print the specified number of records from the specified topic.
    * Poll for as long as necessary.
    * @param topic
    * @param max
    */
  def consumeAndPrint(topic: String, max: Int): Unit = {
    // configure a consumer


    val consumer = new KafkaConsumer[String, String](basicStringStringConsumer);

    // need to subscribe to the topic

    consumer.subscribe(util.Arrays.asList(topic))

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
      records.records(topic).foreach(rec => {
        println("*** [ " + rec.partition() + " ] " + rec.key() + ":" + rec.value())
      })
    }

    println("*** got the expected number of messages")

    consumer.close()
  }

  def basicStringStringProducer : Properties = {
    val config: Properties = new Properties
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    //config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner")
    config
  }

  def basicStringStringConsumer : Properties = {
    SimpleKafkaClient.getBasicStringStringConsumer(server)
  }
}

object SimpleKafkaClient {

  def getBasicStringStringConsumer(server: EmbeddedKafkaServer, group:String = "MyGroup") : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")

    consumerConfig
  }

}

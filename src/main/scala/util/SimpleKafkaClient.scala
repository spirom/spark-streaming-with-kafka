package util

import java.nio.ByteBuffer
import java.util.Properties

import kafka.api.{FetchRequest, FetchRequestBuilder, FetchResponse}
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConversions._

/**
  * Simple utilities for connecting directly to Kafka.
  */
class SimpleKafkaClient(server: EmbeddedKafkaServer) {


  private def printMessages(messageSet: ByteBufferMessageSet) : Unit =  {
    for (messageAndOffset <- messageSet.iterator) {
      val payload = messageAndOffset.message.payload
      val bytes = payload.array()
      System.out.println(new String(bytes, "UTF-8"))
    }
  }

  /**
    * Read and print the specified number of records from the specified topic.
    * Poll for as long as necessary.
    * @param config
    * @param topic
    * @param max
    */
  def consumeAndPrint(config: Properties, topic: String, max: Int): Unit = {
    // configure a consumer

    val clientId = "someclient"

    val simpleConsumer = new SimpleConsumer("localhost",
      server.kbPort,
      10000,
      100000,
      clientId)
    val req = new FetchRequestBuilder()
      .clientId(clientId)
      .addFetch(topic, 0, 0L, 100)
      .build()
    val fetchResponse = simpleConsumer.fetch(req)
    val ms = fetchResponse.messageSet(topic, 0)
    printMessages(ms)

    /*

    val consumer = new KafkaConsumer[String, String](config);

    // need to subscribe to the topic

    consumer.subscribe(topic)

    // and read the records back -- just keep polling until we have read
    // all of them (poll each 100 msec) as the Kafka server may not make
    // them available immediately

    var count = 0;
    var pollCount = 0
    while (count < max && pollCount < 100) {
      println("*** Polling ")

      val records: java.util.Map[String, ConsumerRecords[String, String]] =
        consumer.poll(100)
      pollCount = pollCount + 1
      if (records != null) {
        println(s"*** received ${records.size()} messages")

        count = count + records.size()

        // must specify the topic as we could have subscribed to more than one
        records.get(topic).records().foreach(rec => {
          println("[ " + rec.partition() + " ] " + rec.key() + ":" + rec.value())
        })
      }
    }
*/
    println("*** got the expected number of messages")
  }

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

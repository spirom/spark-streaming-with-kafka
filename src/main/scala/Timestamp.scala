import java.util.{Arrays, Calendar, Properties, TimeZone}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import util.{EmbeddedKafkaServer, SimpleKafkaClient}

/**
  * Record timestamps were introduced into Kafka 0.10 as described in
  * https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
  * and
  * https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index .
  *
  * This example sets up two different topics that handle timestamps differently -- topic A has the timestamp
  * set by the broker when it receives the record, while topic B passes through the timestamp provided in the record
  * (either programmatically when the record was created, as shown here, or otherwise automatically by the producer.)
  *
  * Since the record carries information about where its timestamp originates, its easy to subscribe to the two topics
  * to create a single stream, and then examine the timestamp of every received record and its type.
  */
object Timestamp {
  def main (args: Array[String]) {

    val topicLogAppendTime = "A"
    val topicCreateTime = "B"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topicLogAppendTime, 4, logAppendTime = true)
    kafkaServer.createTopic(topicCreateTime, 4)

    val conf = new SparkConf().setAppName("Timestamp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // this many messages
    val max = 1000

    // Create the stream.
    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topicLogAppendTime, topicCreateTime),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    val timeFormat = new java.text.SimpleDateFormat("HH:mm:ss.SSS")

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(cr => {

        val time = timeFormat.format(cr.timestamp())
        println("Topic [" + cr.topic() + "] Key [" + cr.key + "] Type [" + cr.timestampType().toString +
          "] Timestamp [" + time + "]")
      })
    })

    ssc.start()

    println("*** started termination monitor")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThread = new Thread("Streaming Termination Controller") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val producer = new KafkaProducer[String, String](client.basicStringStringProducer)

        // the two records are created at almost the same time, so should have similar creation time stamps
        // if we didn't provide one, the producer would so so, but then we wouldn't know what it was ...

        val timestamp = Calendar.getInstance().getTime().getTime

        println("Record creation time: " + timeFormat.format(timestamp))

        val record1 = new ProducerRecord(topicLogAppendTime, 1, timestamp, "key1", "value1")
        val record2 = new ProducerRecord(topicCreateTime, 1, timestamp, "key2", "value2")

        Thread.sleep(2000)

        // the two records are sent to the Kafka broker two seconds after they are created, and three seconds apart

        producer.send(record1)
        Thread.sleep(3000)
        producer.send(record2)

        Thread.sleep(5000)
        println("*** requesting streaming termination")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      }
    }
    producerThread.start()

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

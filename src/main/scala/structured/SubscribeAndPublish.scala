package structured

import java.io.File

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import util.{TemporaryDirectories, EmbeddedKafkaServer, SimpleKafkaClient}

/**
  * Two Kafka topics are set up and a KafkaProducer is used to publish to the first topic.
  * Then structured streaming is used to subscribe to that topic and publish a running aggregation to the
  * second topic. Finally structured streaming is used to subscribe to the second topic and print the data received.
  */
object SubscribeAndPublish {

  def main (args: Array[String]) {

    val topic1 = "foo"
    val topic2 = "bar"

    println("*** starting Kafka server")
    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic1, 4)
    kafkaServer.createTopic(topic2, 4)

    Thread.sleep(5000)

    // publish some messages
    println("*** Publishing messages")
    val max = 1000
    val client = new SimpleKafkaClient(kafkaServer)
    val numbers = 1 to max
    val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic1, "key_" + n, "string_" + n))
    }
    Thread.sleep(5000)

    val checkpointPath = kafkaServer.tempDirs.checkpointPath

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("Structured_SubscribeAndPublish")
      .config("spark.master", "local[4]")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
      .option("subscribe", topic1)
      .option("startingOffsets", "earliest")
      .load()

    val counts = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .groupBy()
      .count()

    val publishQuery =
      counts
        .selectExpr("'RunningCount' AS key", "CAST(count AS STRING) AS value")
        .writeStream
        .outputMode("complete")
        .format("kafka")
        .option("checkpointLocation", checkpointPath)
        .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
        .option("topic", topic2)
        .start()

    val ds2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
      .option("subscribe", topic2)
      .option("startingOffsets", "earliest")
      .load()

    val counts2 = ds2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = counts2
      .writeStream
      .trigger(Trigger.ProcessingTime("4 seconds"))
      .format("console")
      .start()

    println("*** done setting up streaming")

    Thread.sleep(2000)

    println("*** publishing more messages")
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic1, "key_" + n, "string_" + n))
    }

    Thread.sleep(8000)

    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    spark.stop()

    println("*** Streaming terminated")

    // stop Kafka
    println("*** Stopping Kafka")
    kafkaServer.stop()

    println("*** done")
  }
}
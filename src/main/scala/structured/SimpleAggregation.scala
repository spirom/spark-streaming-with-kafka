package structured

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import util.{EmbeddedKafkaServer, SimpleKafkaClient}

/**
  * A streaming DataFrame is created from a single Kafka topic, an aggregating query is set up to count
  * occurrences of each key, and the results are streamed to a console. Each batch results in the entire
  * aggregation result to date being output.
  */
object SimpleAggregation {

  def main (args: Array[String]) {

    val topic = "foo"

    println("*** starting Kafka server")
    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    Thread.sleep(5000)

    // publish some messages
    println("*** Publishing messages")
    val max = 1000
    val client = new SimpleKafkaClient(kafkaServer)
    val numbers = 1 to max
    val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "key_" + (n % 4), "string_" + n))
    }
    Thread.sleep(5000)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("Structured_Simple")
      .config("spark.master", "local[4]")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // equivalent of auto.offset.reset which is not allowed here
      .load()

    val counts = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .groupBy("key")
      .count()

    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** publishing more messages")
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "key_" + (n % 4), "string_" + n))
    }

    Thread.sleep(5000)

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
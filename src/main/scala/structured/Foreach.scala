package structured

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import util.{EmbeddedKafkaServer, SimpleKafkaClient}

/**
  * The 'foreach' operation allows arbitrary computations on the output data in way that is both
  * partition-aware (computed on the executors and aware of which partition is being processed) and batch-aware
  * (via a separate invocation for each partition/batch combination.)
  *
  * It is always used by passing the operation an object that implements the 'ForeachWriter' interface. In this
  * example, the object doesn't do any "useful" work: instead it is set up to illustrate its slightly arcane state
  * management by printing its arguments and state in each of the three overridden methods.
  *
  * Each instance of ForeachWriter is used for processing a sequence of partition/batch combinations, but at any point
  * in time is is setup (via a single open() call) to process one partition/batch combination. Then it gets multiple
  * process() calls, providing the the actual data for that partition and batch, and then a single close() call to
  * signal that the partition/batch combination has been completely processed.
  */
object Foreach {

  def main (args: Array[String]) {

    val topic = "foo"

    println("*** starting Kafka server")
    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    Thread.sleep(5000)

    // publish some messages
    println("*** Publishing messages")
    val messageCount = 16
    val client = new SimpleKafkaClient(kafkaServer)
    val numbers = 1 to messageCount
    val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "[1]key_" + n, "[1]string_" + n))
    }
    Thread.sleep(5000)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("Structured_Foreach")
      .config("spark.master", "local[4]")
      .getOrCreate()

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // equivalent of auto.offset.reset which is not allowed here
      .load()

    val counts = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // process the stream using a custom ForeachWriter that simply prints the data and the state of the ForeachWriter
    // in order to illustrate how it works
    val query = counts.writeStream
      .foreach(new ForeachWriter[Row] {

        // Notice the initialization here is very simple, as it gets called on the driver, but never called
        // again on the executor. Any initialization that needs to be called repeatedly on the executor
        // needs to go in the open() method.

        // By using an Option, initializing with None and replacing with None in the close() method, we verify that
        // process() is only ever called between a matched pair of open() and close() calls.
        var myPartition: Option[Long] = None
        var myVersion: Option[Long] = None

        /**
          * Apart from printing the partition and version, we only accept batches from even numbered partitions.
          */
        override def open(partitionId: Long, version: Long): Boolean = {
          myPartition = Some(partitionId)
          myVersion = Some(version)
          println(s"*** ForEachWriter: open partition=[$partitionId] version=[$version]")
          val processThisOne = partitionId % 2 == 0
          // We only accept this partition/batch combination if we return true -- in this case we'll only do so for
          // even numbered partitions. This decision could have been based on the version ID as well.
          processThisOne
        }

        /**
          * Since we've saved the partition and batch IDs, we can see which combination each record comes from.
          * Notice we only get records from even numbered partitions, since we rejected the odd numbered
          * ones in the open() method by returning false.
          */
        override def process(record: Row) : Unit = {
          println(s"*** ForEachWriter: process partition=[$myPartition] version=[$myVersion] record=$record")
        }

        /**
          * Again we've saved the partition and batch IDs, so we can see which combination is being closed.
          * We'll leave error handling for a more advanced example.
          */
        override def close(errorOrNull: Throwable): Unit = {
          println(s"*** ForEachWriter: close partition=[$myPartition] version=[$myVersion]")
          myPartition = None
          myVersion = None
        }
    }).start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** publishing more messages")
    numbers.foreach { n =>
      producer.send(new ProducerRecord(topic, "[2]key_" + n, "[2]string_" + n))
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
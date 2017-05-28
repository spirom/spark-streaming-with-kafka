import java.util.{Arrays, Properties}

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, SimpleKafkaClient}

/**
  * This example demonstrates that exceptions encountered in stream processing are
  * rethrown from the call to awaitTermination().
  * See https://issues.apache.org/jira/browse/SPARK-17397 .
  * Notice this example doesn't even publish any data: the exception is thrown when an empty RDD is received.
  */
object ExceptionPropagation {

  case class SomeException(s: String)  extends Exception(s)

  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    val client = new SimpleKafkaClient(kafkaServer)


    val conf = new SparkConf().setAppName("ExceptionPropagation").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      // throw the custom exception here and see it get caught in the code below
      throw SomeException("error while processing RDD");
    })

    ssc.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
        ssc.stop() // stop it now since we're not blocked
      }
    }

    // stop Spark
    sc.stop()

    // stop Kafka
    kafkaServer.stop()

    println("*** done")
  }
}
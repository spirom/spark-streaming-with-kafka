import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DirectKafkaClient, EmbeddedKafkaServer, SparkKafkaSink}
import org.apache.spark.streaming.kafka.KafkaUtils

object SimpleStreaming {
  def main (args: Array[String]) {

    val topic = "foo"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic)

    val client = new DirectKafkaClient(kafkaServer.getKafkaConnect)


    val conf = new SparkConf().setAppName("SimpelStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val max = 10


    val topicMap =
      Map[String, Int](topic -> 4)
    val kafkaStream =
      KafkaUtils.createStream(ssc, kafkaServer.getZkConnect, "MyGroup", topicMap)

    kafkaStream.foreachRDD(r => {
      println("*** got an RDD")
      r.foreach(s => println(s))
    })

    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run() {
        try {
          ssc.awaitTermination()
        } catch {
          case e: Exception => {
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
          }
        }
        println("*** streaming terminated")
      }
    }.start()

    println("*** started termination monitor")


    // put some data in an RDD
    val numbers = 1 to max
    val numbersRDD = sc.parallelize(numbers, 4)

    val kafkaSink = sc.broadcast(SparkKafkaSink(client.getBasicStringStringProducer(kafkaServer)))

    println("*** producing data")

    Thread.sleep(5000)

    numbersRDD.foreach { n =>
      kafkaSink.value.send(topic, "key_" + n, "string_" + n)
    }

    Thread.sleep(5000)


    ssc.stop()

    kafkaServer.stop()

    sc.stop()

    println("*** done")
  }
}
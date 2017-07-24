package applications.stock_price_feed

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{Arrays, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{EmbeddedKafkaServer, PartitionMapAnalyzer, SimpleKafkaClient}

import scala.collection.{Iterator, mutable}

class TradeData(val symbol: String, val price: Double, val volume: Long) extends Serializable {

}

class ChunkedTradeData(val symbol: String) extends Serializable {
  var trades = 0
  var totalAmount = 0.0
  var totalVolume: Long = 0

  def addTrade(trade: TradeData) : Unit = {
    trades = trades + 1
    totalVolume = totalVolume + trade.volume
    totalAmount = totalAmount + trade.volume * trade.price
  }

  def averagePrice = totalAmount / totalVolume
}

class TradeDataSerializer extends Serializer[TradeData] {

  override def close(): Unit = {}

  override def configure(config: java.util.Map[String, _], isKey: Boolean) : Unit = {}

  override def serialize(topic: String, data: TradeData) : Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(data)
    oos.close
    stream.toByteArray
  }
}

class TradeDataDeserializer extends Deserializer[TradeData] {

  override def close(): Unit = {}

  override def configure(config: java.util.Map[String, _], isKey: Boolean) : Unit = {}

  override def deserialize(topic: String, data: Array[Byte]) : TradeData = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    val value = ois.readObject
    ois.close
    value.asInstanceOf[TradeData]
  }
}



object StockMarketData {

  def getProducer(server: EmbeddedKafkaServer) : Properties = {
    val config: Properties = new Properties
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[TradeDataSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[TradeDataSerializer].getCanonicalName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    config
  }

  def getConsumer(server: EmbeddedKafkaServer, group:String = "MyGroup") : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[TradeDataDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[TradeDataDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig
  }

  def main (args: Array[String]) {

    val topic1 = "SYM1"
    val topic2 = "SYM2"

    // topics are partitioned differently
    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic1, 1)
    kafkaServer.createTopic(topic2, 1)

    val conf = new SparkConf().setAppName("StockMarketData").setMaster("local[10]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // this many messages
    val max = 100

    // Create the stream.
    val props: Properties = getConsumer(kafkaServer)

    val rawDataFeed =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, TradeData](
          Arrays.asList(topic1, topic2),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    rawDataFeed.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())

      PartitionMapAnalyzer.analyze(r)

    })

    def chunkingFunc(i: Iterator[TradeData]) : Iterator[Map[String, ChunkedTradeData]] = {
      val m = new mutable.HashMap[String, ChunkedTradeData]()
      i.foreach {
        case trade: TradeData =>
          if (m.contains(trade.symbol)) {
            m(trade.symbol).addTrade(trade)
          } else {
            val chunked = new ChunkedTradeData(trade.symbol)
            chunked.addTrade(trade)
            m(trade.symbol) = chunked
          }
      }
      Iterator.single(m.toMap)
    }

    val decodedFeed = rawDataFeed.map(cr => cr.value())

    val chunkedDataFeed = decodedFeed.mapPartitions(chunkingFunc, preservePartitioning = true)

    chunkedDataFeed.foreachRDD(rdd => {
      rdd.foreach(m =>
        m.foreach {
          case (symbol, chunk) =>
            println(s"Symbol ${chunk.symbol} Price ${chunk.averagePrice} Volume ${chunk.totalVolume} Trades ${chunk.trades}")
        })
    })

    ssc.start()

    println("*** started streaming context")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThreadTopic1 = new Thread("Producer thread 1") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, TradeData](getProducer(kafkaServer))

        numbers.foreach { n =>
          // NOTE:
          //     1) the keys and values are strings, which is important when receiving them
          //     2) We don't specify which Kafka partition to send to, so a hash of the key
          //        is used to determine this
          producer.send(new ProducerRecord(topic1, new TradeData("SYM1", 12.0, 100)))
        }

      }
    }

    val producerThreadTopic2 = new Thread("Producer thread 2; controlling termination") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, TradeData](getProducer(kafkaServer))

        numbers.foreach { n =>
          // NOTE:
          //     1) the keys and values are strings, which is important when receiving them
          //     2) We don't specify which Kafka partition to send to, so a hash of the key
          //        is used to determine this
          producer.send(new ProducerRecord(topic2,  new TradeData("SYM2", 123.0, 200)))
        }
        Thread.sleep(10000)
        println("*** requesting streaming termination")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      }
    }

    producerThreadTopic1.start()
    producerThreadTopic2.start()

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

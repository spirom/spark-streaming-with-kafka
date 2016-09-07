package util

import java.io.IOException
import java.net.ServerSocket

import info.batey.kafka.unit.KafkaUnit
import kafka.admin.TopicCommand
import org.slf4j.LoggerFactory

/**
  * Use https://github.com/chbatey/kafka-unit to control an embedded Kafka instance.
  */
@throws[IOException]
class EmbeddedKafkaServer() {
  private val LOGGER = LoggerFactory.getLogger(classOf[EmbeddedKafkaServer])
  val zkPort = 39001
  val kbPort = 39002

  private val kafkaServer = new KafkaUnit(zkPort, kbPort)

  private def getEphemeralPort : Int = {
    val socket: ServerSocket = new ServerSocket(0)
    socket.getLocalPort
  }

  def createTopic(topic: String, partitions: Int = 1) {
    val arguments = Array[String](
        "--create",
        "--zookeeper",
        getZkConnect,
        "--replication-factor",
        "1",
        "--partitions",
        "" + partitions,
        "--topic",
        topic
      )
    TopicCommand.main(arguments)
  }

  def getKafkaConnect: String = "localhost:" + kbPort

  def getZkConnect: String = "localhost:" + zkPort

  def start() {
    LOGGER.info("starting on [{} {}]", zkPort, kbPort)
    kafkaServer.startup()
  }

  def stop() {
    kafkaServer.shutdown()
  }
}

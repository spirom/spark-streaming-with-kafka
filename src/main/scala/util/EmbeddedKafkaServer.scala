package util

import java.io.IOException
import java.net.ServerSocket

import com.typesafe.scalalogging.Logger
import info.batey.kafka.unit.KafkaUnit
import kafka.admin.TopicCommand


/**
  * Use https://github.com/chbatey/kafka-unit to control an embedded Kafka instance.
  */
@throws[IOException]
class EmbeddedKafkaServer() {
  private val LOGGER = Logger[EmbeddedKafkaServer]
  val zkPort = 39001
  val kbPort = 39002

  private val kafkaServer = new KafkaUnit(zkPort, kbPort)

  private def getEphemeralPort : Int = {
    val socket: ServerSocket = new ServerSocket(0)
    socket.getLocalPort
  }

  def createTopic(topic: String, partitions: Int = 1) {
    LOGGER.debug("creating topic [$topic]")
    kafkaServer.createTopic(topic, partitions)
    LOGGER.debug("finished creating topic [$topic]")
  }

  def getKafkaConnect: String = "localhost:" + kbPort

  def getZkConnect: String = "localhost:" + zkPort

  def start() {
    LOGGER.info("starting on [$zkPort $kbPort]")
    kafkaServer.startup()
  }

  def stop() {
    kafkaServer.shutdown()
  }
}

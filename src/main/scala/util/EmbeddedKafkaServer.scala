package util

import java.io.IOException
import java.net.ServerSocket

import com.typesafe.scalalogging.Logger
import info.batey.kafka.unit.KafkaUnit
import kafka.admin.TopicCommand
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils

import scala.collection.mutable.ArrayBuffer

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

  def createTopic(topic: String, partitions: Int = 1, logAppendTime: Boolean = false) : Unit = {
    LOGGER.debug(s"Creating [$topic]")

    val arguments = Array[String](
      "--create",
      "--topic",
      topic
    ) ++ (
    if (logAppendTime) {
      Array[String]("--config", "message.timestamp.type=LogAppendTime")
    } else {
      Array[String]()
    }) ++ Array[String](
      "--partitions",
      "" + partitions,
      "--replication-factor",
      "1"
    )

    val opts = new TopicCommand.TopicCommandOptions(arguments)

    val zkUtils = ZkUtils.apply(getZkConnect,
      30000, 30000, JaasUtils.isZkSecurityEnabled)

    TopicCommand.createTopic(zkUtils, opts)

    LOGGER.debug(s"Finished creating topic [$topic]")
  }

  def addPartitions(topic: String, partitions: Int) : Unit = {
    LOGGER.debug(s"Adding [$partitions] partitions to [$topic]")

    val arguments = Array[String](
      "--alter",
      "--topic",
      topic,
      "--partitions",
      "" + partitions
    )

   val opts = new TopicCommand.TopicCommandOptions(arguments)

    val zkUtils = ZkUtils.apply(getZkConnect,
      30000, 30000, JaasUtils.isZkSecurityEnabled)

    TopicCommand.alterTopic(zkUtils, opts)

    LOGGER.debug(s"Finished adding [$partitions] partitions to [$topic]")
  }

  def getKafkaConnect: String = "localhost:" + kbPort

  def getZkConnect: String = "localhost:" + zkPort

  def start() {
    LOGGER.info(s"starting on [$zkPort $kbPort]")
    kafkaServer.startup()
  }

  def stop() {
    kafkaServer.shutdown()
  }
}

package util

import java.io.IOException

import info.batey.kafka.unit.KafkaUnit

@throws[IOException]
class EmbeddedKafkaServer() {
  private val kafkaServer = new KafkaUnit

  def createTopic(topic: String, partitions: Int = 1) {
    kafkaServer.createTopic(topic, partitions)
  }

  def getKafkaConnect: String = kafkaServer.getKafkaConnect

  def getZkConnect: String = "localhost:" + kafkaServer.getZkPort

  def start() {
    kafkaServer.startup()
  }

  def stop() {
    kafkaServer.shutdown()
  }
}

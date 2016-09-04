package util

import java.io.IOException

import info.batey.kafka.unit.KafkaUnit

@throws[IOException]
class EmbeddedKafkaServer() {
  private val kafkaServer = new KafkaUnit

  def createTopic(topic: String) {
    kafkaServer.createTopic(topic)
  }

  def getKafkaConnect: String = kafkaServer.getKafkaConnect

  def start() {
    kafkaServer.startup()
  }

  def stop() {
    kafkaServer.shutdown()
  }
}

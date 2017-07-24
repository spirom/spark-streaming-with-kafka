package util

/**
  * Run this first to verify that the embedded Kafka setup is working for you.
  * It starts an embedded Kafka server, creates a topic, publishes some messages,
  * reads them back and shuts down the embedded server.
  */

object DirectServerDemo {
  def main (args: Array[String]) {

    val topic = "foo"

    println("*** about to start embedded Kafka server")

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()

    println("*** server started")

    kafkaServer.createTopic(topic, 4)

    println("*** topic [" + topic + "] created")

    Thread.sleep(5000)

    val kafkaClient = new SimpleKafkaClient(kafkaServer)

    println("*** about to produce messages")

    kafkaClient.send(topic, Seq(
      ("Key_1", "Value_1"),
      ("Key_2", "Value_2"),
      ("Key_3", "Value_3"),
      ("Key_4", "Value_4"),
      ("Key_5", "Value_5")
    ))

    println("*** produced messages")

    Thread.sleep(5000)

    println("*** about to consume messages")

    kafkaClient.consumeAndPrint(
      topic,
      5)

    println("*** stopping embedded Kafka server")

    kafkaServer.stop()

    println("*** done")
  }
}

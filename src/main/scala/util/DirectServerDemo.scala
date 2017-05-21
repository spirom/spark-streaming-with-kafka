package util


object DirectServerDemo {
  def main (args: Array[String]) {

    val topic = "foo"

    println("*** about to start embedded Kafka server")

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()

    println("*** server started")

    kafkaServer.createTopic(topic, 4)

    println("*** topic [" + topic + "] vreated")

    Thread.sleep(5000)

    println("*** stopping embedded Kafka server")

    kafkaServer.stop()

    println("*** done")
  }
}

import info.batey.kafka.unit.KafkaUnit;

import java.io.IOException;

public class EmbeddedKafkaServer {

  private KafkaUnit _kafkaServer;

  public EmbeddedKafkaServer() throws IOException {
      _kafkaServer = new KafkaUnit();
  }

  public void createTopic(String topic) {
    _kafkaServer.createTopic(topic);
  }

  public String getKafkaConnect() { return _kafkaServer.getKafkaConnect(); }

  public void start() {
    _kafkaServer.startup();
  }

  public void stop() {
    _kafkaServer.shutdown();
  }
}

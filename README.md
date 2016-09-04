# Self-contained examples of Spark streaming integrated with Kafka

## Dependencies

The project was created with IntelliJ Idea 14 Community Edition,
JDK 1.7, Scala 2.11.2, Kafka 0.10.0.0, kafka-unit 0.6 and Spark 2.0.0 on Ubuntu Linux.

It uses the package park-streaming-kafka-0-8 for SPark Streaming integration with Kafka.
This is to obtain access to the stable API, as described
[here](https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/streaming-kafka-integration.html).

## Utilities

| File                  | Purpose    |
|---------------------------------|-----------------------|
| util/EmbeddedKafkaServer.scala | Starting and stopping an embedded Kafka server and create topics. |
| util/DirectKafkaClient.scala | Directly connect to Kafka without usign Spark. |
| util/Monitoring.scala | Support for monitoring a StreamingContext to show stack traces for any exceptions. |
| util/SparkKafkaSink.scala | Support for publishing to Kafka topic in parallel from Spark. |

## Examples

| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| UtilDemo.scala | How to use the Kafka utilities described above. |
| BatchProducer.scala | Publishing to a Kafka topic in parallel from a Spark batch job. |
| SimpleStreaming.scala | Simple way to set up streaming from a Kafka topic. |
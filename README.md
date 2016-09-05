# Self-contained examples of Spark streaming integrated with Kafka

The goal of this project is to make it easy to experiment with Spark Streaming based on Kafka,
by creating examples that run against an embedded Kafka server and an embedded Spark instance.
Of course, in making everything easy to work with we also make it perform poorly. It would be a
really bad idea to try to learn anything about performance from this project: it's all
about functionality, although we sometimes get insight into performance issues by understanding
the way the
code interacts with RDD partitioning in Spark and topic partitioning in Kafka.

## Dependencies

The project was created with IntelliJ Idea 14 Community Edition,
JDK 1.7, Scala 2.11.2, Kafka 0.10.0.0, kafka-unit 0.6 and Spark 2.0.0 on Ubuntu Linux.

It uses the package spark-streaming-kafka-0-8 for Spark Streaming integration with Kafka.
This is to obtain access to the stable API -- the details
behind this are documented in the Spark 2.1.0 documentation, which is not yet available.

## Utilities

| File                  | Purpose    |
|---------------------------------|-----------------------|
| util/EmbeddedKafkaServer.scala | Starting and stopping an embedded Kafka server and create topics. |
| util/SimpleKafkaClient.scala | Directly connect to Kafka without using Spark. |
| util/SparkKafkaSink.scala | Support for publishing to Kafka topic in parallel from Spark. |

## Examples

| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| UtilDemo.scala | How to use the Kafka utilities described above. |
| BatchProducer.scala | Publishing to a Kafka topic in parallel from a Spark batch job. |
| SimpleStreaming.scala | Simple way to set up streaming from a Kafka topic. |
| ExceptionPropagation.scala | Show how call to awaitTermination() throws propagated exceptions. |
# Self-contained examples of Spark streaming integrated with Kafka

The goal of this project is to make it easy to experiment with Spark Streaming based on Kafka,
by creating examples that run against an embedded Kafka server and an embedded Spark instance.
Of course, in making everything easy to work with we also make it perform poorly. It would be a
really bad idea to try to learn anything about performance from this project: it's all
about functionality, although we sometimes get insight into performance issues by understanding
the way the
code interacts with RDD partitioning in Spark and topic partitioning in Kafka.

## Dependencies

The project was created with IntelliJ Idea 14 Community Edition. It is known to work with
JDK 1.7, Scala 2.11.2, Kafka 0.8.2.1, kafka-unit 0.2 and Spark 2.1.0 on Ubuntu Linux.

It uses the package spark-streaming-kafka-0-8 for Spark Streaming integration with Kafka.
This is to obtain access to the stable API -- the details
behind this are explained in the
[Spark 2.1.0 documentation](https://spark.apache.org/docs/2.1.0/streaming-kafka-integration.html).

## _Note:_ Using The Experimental Kafka 0.10.0 APIs

I'm trying to get these working on the [kafka0.10](https://github.com/spirom/spark-streaming-with-kafka/tree/kafka0.10) branch, and probably won't merge
that branch back to master until the new APIs become mainstream, which
probably won't be anytime soon. Only the
[util/DirectServerDemo.scala](https://github.com/spirom/spark-streaming-with-kafka/tree/kafka0.10/src/main/scala/util/DirectServerDemo.scala)
sanity check and the
[SimpleStreaming.scala](https://github.com/spirom/spark-streaming-with-kafka/tree/kafka0.10/src/main/scala/SimpleStreaming.scala)
example work right now.

Again, the details are explained in the
[Spark 2.1.0 documentation](https://spark.apache.org/docs/2.1.0/streaming-kafka-integration.html).


## Utilities

| File                  | Purpose    |
|---------------------------------|-----------------------|
| [util/EmbeddedKafkaServer.scala](src/main/scala/util/EmbeddedKafkaServer.scala) | Starting and stopping an embedded Kafka server and create topics. |
| [util/SimpleKafkaClient.scala](src/main/scala/util/SimpleKafkaClient.scala) | Directly connect to Kafka without using Spark. |
| [util/SparkKafkaSink.scala](src/main/scala/util/SparkKafkaSink.scala) | Support for publishing to Kafka topic in parallel from Spark. |

## Examples

| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| [SimpleStreaming.scala](src/main/scala/SimpleStreaming.scala) | Simple way to set up streaming from a Kafka topic. |
| [PartitionedStreaming.scala](src/main/scala/PartitionedStreaming.scala) | RDD partitioning is aware of Kafka topic partitioning. |
| [ControlledPartitioning.scala](src/main/scala/ControlledPartitioning.scala) | When publishing to the topic, explicitly assign each record to a partition. |
| [MultipleConsumerGroups.scala](src/main/scala/MultipleConsumerGroups.scala) | Two streams subscribing to the same topic via two consumer groups see all the same data. |
| [ExceptionPropagation.scala](src/main/scala/ExceptionPropagation.scala) | Show how call to awaitTermination() throws propagated exceptions. |
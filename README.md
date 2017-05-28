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

## Using the Experimental Kafka 0.10.0 APIs

I'm exploring the new experimental APIs based on Kafka 0.10 on the
[kafka0.10](https://github.com/spirom/spark-streaming-with-kafka/tree/kafka0.10) branch, and probably won't merge
that branch back to master until the new APIs become mainstream, which
probably won't be anytime soon. The functional differences are causing me to reorganize the examples somewhat.

Again, the details are explained in the
[Spark 2.1.0 documentation](https://spark.apache.org/docs/2.1.0/streaming-kafka-integration.html).


## Utilities

| File                  | Purpose    |
|---------------------------------|-----------------------|
| [util/DirectServerDemo.scala](src/main/scala/util/DirectServerDemo.scala) | **Run this first as a Spark-free sanity check for embedded server and clients.** |
| [util/EmbeddedKafkaServer.scala](src/main/scala/util/EmbeddedKafkaServer.scala) | Starting and stopping an embedded Kafka server and create topics. |
| [util/SimpleKafkaClient.scala](src/main/scala/util/SimpleKafkaClient.scala) | Directly connect to Kafka without using Spark. |
| [util/SparkKafkaSink.scala](src/main/scala/util/SparkKafkaSink.scala) | Support for publishing to Kafka topic in parallel from Spark. |

## Basic Examples

| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| [SimpleStreaming.scala](src/main/scala/SimpleStreaming.scala) | **Simple way to set up streaming from a Kafka topic.** |
| [ExceptionPropagation.scala](src/main/scala/ExceptionPropagation.scala) | Show how call to awaitTermination() throws propagated exceptions. |

## Partitioning Examples

Partitioning is an important factor in determining the scalability oif Kafka-based streaming applications.
In this set of examples you can see the relationship between a number of facets of partitioning.
* The number of partitions in the RDD that is being published to a topic
* The number of partitions of the topic itself (usually specified at topic creation)
* THe number of partitions in the RDDs created by the Kafka stream
* Whether and how messages move between partitions when they are transferred


| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| [SendWithDifferentPartitioning.scala](src/main/scala/SendWithDifferentPartitioning.scala) | Send to a topic with different number of partitions. |
| [ControlledPartitioning.scala](src/main/scala/ControlledPartitioning.scala) | When publishing to the topic, explicitly assign each record to a partition. |

## Other Group Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td><a href="src/main/scala/MultipleConsumerGroups.scala">MultipleConsumerGroups.scala</a></td>
<td>Two streams subscribing to the same topic via two consumer groups see all the same data.</td>
</tr>
<tr>
<td><a href ="src/main/scala/MultipleStreams.scala">MultipleStreams.scala</a></td>
<td>Two streams subscribing to the same topic via a single consumer group divide up the data.
There's an interesting partitioning interaction here as the streams each get data from two fo the four topic
partitions, and each produce RDDs with two partitions each. </td>
</tr>
</table>


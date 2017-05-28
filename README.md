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
JDK 1.7, Scala 2.11.2, kafka-unit 0.2 and Spark 2.1.0 with its Kafka 0.10 shim library on Ubuntu Linux.

It uses the package spark-streaming-kafka-0-8 for Spark Streaming integration with Kafka.
This is to obtain access to the stable API -- the details
behind this are explained in the
[Spark 2.1.0 documentation](https://spark.apache.org/docs/2.1.0/streaming-kafka-integration.html).

## Using the Experimental Kafka 0.10.0 APIs

I'm exploring the new experimental APIs (Direct DStream instead of Receiver DStream) based on Kafka 0.10 on the
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
| [SimpleStreaming.scala](src/main/scala/SimpleStreaming.scala) | **Simple way to set up streaming from a Kafka topic.** While this program also publishes to the topic, the publishing does not involve Spark |
| [ExceptionPropagation.scala](src/main/scala/ExceptionPropagation.scala) | Show how call to awaitTermination() throws propagated exceptions. |

## Partitioning Examples

Partitioning is an important factor in determining the scalability oif Kafka-based streaming applications.
In this set of examples you can see the relationship between a number of facets of partitioning.
* The number of partitions in the RDD that is being published to a topic -- if indeed this involves an RDD, as the data is often published from a non-Spark application
* The number of partitions of the topic itself (usually specified at topic creation)
* THe number of partitions in the RDDs created by the Kafka stream
* Whether and how messages move between partitions when they are transferred

When running these examples, look for:
* The topic partition number that is printed with each ConsumerRecord
* After all the records are printed, the number of partitions in the resulting RDD and size of each partition. For example:
    *** 4 partitions
    *** partition size = 253
    *** partition size = 252
    *** partition size = 258
    *** partition size = 237


Another way these examples differ from the basic examples above is that Spark is used to publish to the topic.
Perhaps surprisingly, this is not completely straightforward, and relies on [util/SparkKafkaSink.scala](src/main/scala/util/SparkKafkaSink.scala).
An alternative approach to this can be found [here](https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/09%20Write%20Output%20To%20Kafka.html).


| File                  | What's Illustrated    |
|---------------------------------|-----------------------|
| [SimpleStreamingFromRDD.scala](src/main/scala/SimpleStreamingFromRDD.scala) | Data is published by Spark from an RDD, but is repartitioned even through the publishing RDD and the topic have the same number of partitions. |
| [SendWithDifferentPartitioning.scala](src/main/scala/SendWithDifferentPartitioning.scala) | Send to a topic with different number of partitions. |
| [ControlledPartitioning.scala](src/main/scala/ControlledPartitioning.scala) | When publishing to the topic, explicitly assign each record to a partition. |

## Other Examples

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

## Possible future examples
* Multiple topics
* Multiple streaming contexts
* Spark 2.1 structured streaming


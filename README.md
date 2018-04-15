# Self-contained examples of Spark streaming integrated with Kafka

[![Build Status](https://travis-ci.org/spirom/spark-streaming-with-kafka.svg?branch=master)](https://travis-ci.org/spirom/spark-streaming-with-kafka)

The goal of this project is to make it easy to experiment with Spark Streaming based on Kafka,
by creating examples that run against an embedded Kafka server and an embedded Spark instance.
Of course, in making everything easy to work with we also make it perform poorly. It would be a
really bad idea to try to learn anything about performance from this project: it's all
about functionality, although we sometimes get insight into performance issues by understanding
the way the
code interacts with RDD partitioning in Spark and topic partitioning in Kafka.

## Related projects

This project is derived from the
[LearningSpark project](https://github.com/spirom/LearningSpark) which explores the full range of Spark APIs from the
viewpoint of Scala developers. There is a corresponding, but much less comprehensive Java version at
[learning-spark-with-java](https://github.com/spirom/learning-spark-with-java).

The [spark-data-sources](https://github.com/spirom/spark-data-sources) project is focused on
the new experimental APIs introduced in Spark 2.3.0 for developing adapters for
external data sources of
various kinds. This API is essentially a Java API (developed in Java) to avoid forcing
developers to adopt Scala for their data source adapters. Consequently, the example data sources
in this project are written in Java, but both Java and Scala usage examples are provided.

## Dependencies

The project was created with IntelliJ Idea 14 Community Edition. It is known to work with
JDK 1.8, Scala 2.11.12, and Spark 2.3.0 with its Kafka 0.10 shim library on Ubuntu Linux.

It uses the Direct DStream package spark-streaming-kafka-0-10 for
Spark Streaming integration with Kafka 0.10.0.1. The details behind this are explained in the
[Spark 2.3.0 documentation](https://spark.apache.org/docs/2.3.0/streaming-kafka-integration.html).

Note that, with the release of Spark 2.3.0, the formerly stable Receiver DStream APIs are now deprecated, and the
formerly experimental Direct DStream APIs are now stable.

## Using the deprecated (Receiver DStream) Kafka 0.8.0 APIs

I've kept around the examples for the older, stable Kafka integration on the
[kafka0.8](https://github.com/spirom/spark-streaming-with-kafka/tree/kafka0.8) branch

# Structured Streaming

There's a [separate set of examples](src/main/scala/structured) for
Kafka integration with the new Structured Streaming features (mainstream as of Spark 2.2).

## Utilities

<table>
<tr><th>File</th><th>Purpose</th></tr>
<tr>
<td valign="top"><a href="src/main/scala/util/DirectServerDemo.scala">util/DirectServerDemo.scala</a></td>
<td><b>Run this first as a Spark-free sanity check for embedded server and clients.</b></td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/EmbeddedKafkaServer.scala">util/EmbeddedKafkaServer.scala</a></td>
<td>Starting and stopping an embedded Kafka server, and creating and modifying topics.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/EmbeddedZookeeper.scala">util/EmbeddedZookeeper.scala</a></td>
<td>Starting and stopping an embedded Zookeeper.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/PartitionMapAnalyzer.scala">util/PartitionMapAnalyzer.scala</a></td>
<td>Support for understanding how subscribed Kafka topics and their Kafka partitions map to partitions in the
RDD that is emitted by the Spark stream.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/SimpleKafkaClient.scala">util/SimpleKafkaClient.scala</a></td>
<td>Directly connect to Kafka without using Spark.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/SparkKafkaSink.scala">util/SparkKafkaSink.scala</a></td>
<td>Support for publishing to Kafka topic in parallel from Spark.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/util/TemporaryDirectories.scala">util/TemporaryDirectories.scala</a></td>
<td>Support for creating and cleaning up temporary directories needed for Kafka broker, Zookeeper and
Spark streaming.</td>
</tr>
</table>

## Basic Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td valign="top"><a href="src/main/scala/SimpleStreaming.scala">SimpleStreaming.scala</a></td>
<td><b>Simple way to set up streaming from a Kafka topic.</b> While this program also publishes to the topic, the publishing does not involve Spark</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/ExceptionPropagation.scala">ExceptionPropagation.scala</a></td>
<td>Show how call to awaitTermination() throws propagated exceptions.</td>
</tr>
</table>

## Partitioning Examples

Partitioning is an important factor in determining the scalability oif Kafka-based streaming applications.
In this set of examples you can see the relationship between a number of facets of partitioning.
* The number of partitions in the RDD that is being published to a topic -- if indeed this involves an RDD, as the data is often published from a non-Spark application
* The number of partitions of the topic itself (usually specified at topic creation)
* THe number of partitions in the RDDs created by the Kafka stream
* Whether and how messages move between partitions when they are transferred

When running these examples, look for:
* The topic partition number that is printed with each ConsumerRecord
* After all the records are printed, the number of partitions in the resulting RDD and size of each partition. For example:</br>
    *** 4 partitions</br>
    *** partition size = 253</br>
    *** partition size = 252</br>
    *** partition size = 258</br>
    *** partition size = 237</br>


Another way these examples differ from the basic examples above is that Spark is used to publish to the topic.
Perhaps surprisingly, this is not completely straightforward, and relies on [util/SparkKafkaSink.scala](src/main/scala/util/SparkKafkaSink.scala).
An alternative approach to this can be found [here](https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/09%20Write%20Output%20To%20Kafka.html).


<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td valign="top"><a href="src/main/scala/SimpleStreamingFromRDD.scala">SimpleStreamingFromRDD.scala</a></td>
<td>Data is published by Spark from an RDD, but is repartitioned even through the publishing RDD and the topic have the same number of partitions.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/SendWithDifferentPartitioning.scala">SendWithDifferentPartitioning.scala</a></td>
<td>Send to a topic with different number of partitions.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/ControlledPartitioning.scala">ControlledPartitioning.scala</a></td>
<td>When publishing to the topic, explicitly assign each record to a partition.</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/AddPartitionsWhileStreaming.scala">AddPartitionsWhileStreaming.scala</a></td>
<td><p>Partitions can be added to a Kafka topic dynamically. This example shows that an existing stream
will not see the data published to the new partitions, and only when the existing streaming context is terminated
and a new stream is started from a new context will that data be delivered.</p>
<p>
The topic is created with three partitions, and so each RDD the stream produces has three partitions as well,
even after two more partitions are added to the topic. This is what's received after the first 500 records
are published to the topic while it has only three partitions:</p>
<pre>
[1] *** got an RDD, size = 500
[1] *** 3 partitions
[1] *** partition size = 155
[1] *** partition size = 173
[1] *** partition size = 172
</pre>
<p>When two partitions are added and another 500 messages are published, this is what's received
(note both the number of partitions and the number of messages):</p>
<pre>
[1] *** got an RDD, size = 288
[1] *** 3 partitions
[1] *** partition size = 98
[1] *** partition size = 89
[1] *** partition size = 101
</pre>

<p>When a new stream is subsequently created, the RDDs produced
have five partitions, but only two of them contain data, as all the data has been drained from the initial three
partitions of the topic, by the first stream. Now all 500 messages (288 + 212) from the second set have been delivered. </p>

<pre>
[2] *** got an RDD, size = 212
[2] *** 5 partitions
[2] *** partition size = 0
[2] *** partition size = 0
[2] *** partition size = 0
[2] *** partition size = 112
[2] *** partition size = 100
</pre>

</td>
</tr>
</table>

## Other Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td valign="top"><a href="src/main/scala/MultipleConsumerGroups.scala">MultipleConsumerGroups.scala</a></td>
<td>Two streams subscribing to the same topic via two consumer groups see all the same data.</td>
</tr>
<tr>
<td valign="top"><a href ="src/main/scala/MultipleStreams.scala">MultipleStreams.scala</a></td>
<td>Two streams subscribing to the same topic via a single consumer group divide up the data.
There's an interesting partitioning interaction here as the streams each get data from two fo the four topic
partitions, and each produce RDDs with two partitions each. </td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/MultipleTopics.scala">MultipleTopics.scala</a></td>
<td>A single stream subscribing to the two topics receives data from both of them.
The partitioning behavior here is quite interesting.
<ul>
<li>The topics have three and six partitions respectively.</li>
<li>Each RDD has nine partitions.</li>
<li>Each RDD partition receives data from exactly one partition of one topic.</li>
</ul>
Hence the output of the PartitionMapAnalyzer:
<pre>
*** got an RDD, size = 200
*** 9 partitions
*** partition 1 has 27 records
*** rdd partition = 1, topic = foo, topic partition = 0, record count = 27.
*** partition 2 has 15 records
*** rdd partition = 2, topic = bar, topic partition = 1, record count = 15.
*** partition 3 has 17 records
*** rdd partition = 3, topic = bar, topic partition = 0, record count = 17.
*** partition 4 has 39 records
*** rdd partition = 4, topic = foo, topic partition = 1, record count = 39.
*** partition 5 has 34 records
*** rdd partition = 5, topic = foo, topic partition = 2, record count = 34.
*** partition 6 has 11 records
*** rdd partition = 6, topic = bar, topic partition = 3, record count = 11.
*** partition 7 has 18 records
*** rdd partition = 7, topic = bar, topic partition = 4, record count = 18.
*** partition 8 has 20 records
*** rdd partition = 8, topic = bar, topic partition = 2, record count = 20.
</pre>
</td>
</tr>
<tr>
<td valign="top"><a href="src/main/scala/Timestamp.scala">Timestamp.scala</a></td>
<td>
<p>Record timestamps were introduced into Kafka 0.10 as described in
<a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message">KIP-32</a>
and
<a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index">KIP-33</a>.</p>

<p>This example sets up two different topics that handle timestamps differently -- topic A has the timestamp
set by the broker when it receives the record, while topic B passes through the timestamp provided in the record
(either programmatically when the record was created, as shown here, or otherwise automatically by the producer.)</p>

<p>Since the record carries information about where its timestamp originates, its easy to subscribe to the two topics
to create a single stream, and then examine the timestamp of every received record and its type.</p>

<p>NOTE: The use of timestamps to filter topics in the broker, as introduced in Kafka 0.10.1, is blocked on
<a href="https://issues.apache.org/jira/browse/SPARK-18057">SPARK-18057</a>.</p>
</td>
</tr>
</table>




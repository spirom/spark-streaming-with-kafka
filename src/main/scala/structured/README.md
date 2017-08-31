# Structured Streaming

Structured Streaming (an Alpha feature in Spark 2.1, but a mainstream feature in Spark 2.2) has its own
[programming guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
in the official documentation. There's also a [Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

## Basic Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td><a href="Simple.scala">Simple.scala</a></td>
<td>
<p>A very simple example of structured streaming from a Kafka source, where the messages
are produced directly via calls to a KafkaProducer. A streaming DataFrame is created from a
single Kafka topic, and feeds all the data received to a streaming computation that outputs it to a console.</p>
<p>Note that writing all the incremental data in each batch to output only makes sense because there is no
aggregation performed. In subsequent examples with aggregation this will not be possible.</p>
</td>
</tr>
<tr>
<td><a href="SimpleAggregation.scala">SimpleAggregation.scala</a></td>
<td>
<p>A streaming DataFrame is created from a single Kafka topic, an aggregating query is set up to count
occurrences of each key, and the results are streamed to a console. Each batch results in the entire
aggregation result to date being output.</p>
</td>
</tr>
<tr>
<td><a href="SubscribeAndPublish.scala">SubscribeAndPublish.scala</a></td>
<td>
<p>Two Kafka topics are set up and a KafkaProducer is used to publish to the first topic.
Then structured streaming is used to subscribe to that topic and publish a running aggregation to the
second topic. Finally structured streaming is used to subscribe to the second topic and print the data received.
</p>
</td>
</tr>

<tr>
<td><a href="Foreach.scala">Foreach.scala</a></td>
<td>
<p>
The 'foreach' operation allows arbitrary computations on the output data in way that is both
partition-aware (computed on the executors and aware of which partition is being processed) and batch-aware
(via a separate invocation for each partition/batch combination.)
<p></p>
It is always used by passing the operation an object that implements the 'ForeachWriter' interface. In this
example, the object doesn't do any "useful" work: instead it is set up to illustrate its slightly arcane state
management by printing its arguments and state in each of the three overridden methods.
<p></p>
Each instance of ForeachWriter is used for processing a sequence of partition/batch combinations, but at any point
in time is is setup (via a single open() call) to process one partition/batch combination. Then it gets multiple
process() calls, providing the the actual data for that partition and batch, and then a single close() call to
signal that the partition/batch combination has been completely processed.
</p>
</td>
</tr>


</table>


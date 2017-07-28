# Structured Streaming

Structured Streaming (an Alpha feature in Spark 2.1) is has its own
[programming guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
in the official documentation. There's also a [Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

## Basic Examples

<table>
<tr><th>File</th><th>What's Illustrated</th></tr>
<tr>
<td><a href="src/main/scala/structured/Simple.scala">Simple.scala</a></td>
<td>
<p>A very simple example of structured streaming from a Kafka source, where the messages
are produced directly via calls to a KafkaProducer. A streaming DataFrame is created from a
single Kafka topic, and feeds all the data received to a streaming computation that outputs it to a console.</p>
<p>Note that writing all the incremental data in each batch to output only makes sense because there is no
aggregation performed. In subsequent examples with aggregation this will not be possible.</p>
</td>
</tr>
<tr>
<td><a href="src/main/scala/structured/SimpleAggregation.scala">SimpleAggregation.scala</a></td>
<td>
<p>A streaming DataFrame is created from a single Kafka topic, an aggregating query is set up to count
occurrences of each key, and the results are streamed to a console. Each batch results in the entire
aggregation result to date being output.</p>
</td>
</tr>
<tr>
<td><a href="src/main/scala/structured/SubscribeAndPublish.scala">SubscribeAndPublish.scala</a></td>
<td>
<p>Two Kafka topics are set up and a KafkaProducer is used to publish to the first topic.
Then structured streaming is used to subscribe to that topic and publish a running aggregation to the
second topic. Finally structured streaming is used to subscribe to the second topic and print the data received.
</p>
</td>
</tr>

</table>


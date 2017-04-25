name := "spark-streaming-with-kafka"

version := "1.0"

scalaVersion := "2.11.8"

fork := true


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1"

// delete this after upgrading Kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"

libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.2"

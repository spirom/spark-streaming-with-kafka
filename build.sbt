name := "spark-streaming-with-kafka"

version := "1.0"

scalaVersion := "2.11.8"

fork := true


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"

libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.6"

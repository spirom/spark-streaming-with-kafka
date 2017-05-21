name := "spark-streaming-with-kafka"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.0"

libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.6"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"



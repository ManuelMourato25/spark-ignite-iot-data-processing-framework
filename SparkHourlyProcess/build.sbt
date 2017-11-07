name := "SensorProcessing"

version := "1.0"

scalaVersion := "2.10.6"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "2.2.0"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.4"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.2.0"
//libraryDependencies += "org.apache.ignite" % "ignite-spark" % "2.2.0"
libraryDependencies += "org.apache.ignite" % "ignite-spring" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.ignite/ignite-indexing
libraryDependencies += "org.apache.ignite" % "ignite-indexing" % "2.2.0"
// https://mvnrepository.com/artifact/net.liftweb/lift-webkit_2.10
libraryDependencies += "net.liftweb" % "lift-webkit_2.10" % "2.6.2"




name := "kafka_spark_streaming_hw"

version := "0.1"

scalaVersion := "2.12.13"
val sparkVersion = "3.1.1"
val kafkaVersion = "2.8.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  //"org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion
)

//"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
//"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
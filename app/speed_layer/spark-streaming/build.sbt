name := "spark-streaming-app"

version := "0.1"

scalaVersion := "2.12.18"

// Spark version (must match the one in your Docker image and spark-submit)
val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1"
)

// Optional: to allow using Spark with Hadoop in HDFS environment
ThisBuild / resolvers += Resolver.mavenLocal

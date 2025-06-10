name := "spark-streaming"

version := "0.1"

scalaVersion := "2.12.15" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0"
)

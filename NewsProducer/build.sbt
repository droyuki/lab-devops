name := "NewsProducer"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.1"
)
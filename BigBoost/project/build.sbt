name := "project"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
  //Hadoop
  "org.apache.hadoop" % "hadoop-client" % "2.7.1",
  //Spark
  "org.apache.spark" % "spark-core_2.11" % "1.4.1",
  "org.apache.spark" %% "spark-streaming" % "1.4.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
  //Spark SQL
  "org.apache.spark" % "spark-sql_2.11" % "1.4.1",
  "mysql" % "mysql-connector-java" % "5.1.6"
)
assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

test in assembly := {}
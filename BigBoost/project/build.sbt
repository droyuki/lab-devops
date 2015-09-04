name := "project"
version := "1.0"
scalaVersion := "2.10.5"
fork := true

libraryDependencies ++= Dependencies.SparkLib
libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.apache.httpcomponents" % "httpclient" % "4.4.1",
  "org.json4s" %% "json4s-jackson" % "3.2.10"
)

//some libs will force upgrade scala version.
ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}
baseAssemblySettings
assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

test in assembly := {}

fork in run := true
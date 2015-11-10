import sbt._

object Version {
  val akka      = "2.3.9"
  val logback   = "1.1.2"
  val mockito   = "1.10.19"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
  val spark     = "1.5.0"
}

object Library {
  val akkaActor      	= "com.typesafe.akka" %% "akka-actor"               % Version.akka
  val akkaTestKit    	= "com.typesafe.akka" %% "akka-testkit"             % Version.akka
  val logbackClassic 	= "ch.qos.logback"    %  "logback-classic"          % Version.logback
  val mockitoAll     	= "org.mockito"       %  "mockito-all"              % Version.mockito
  val scalaTest      	= "org.scalatest"     %% "scalatest"                % Version.scalaTest
  val slf4jApi       	= "org.slf4j"         %  "slf4j-api"                % Version.slf4j
  val sparkStreaming 	= "org.apache.spark"  %% "spark-streaming"          % Version.spark
  val sparkCore      	= "org.apache.spark"  %% "spark-core"               % Version.spark
  val sparkSql       	= "org.apache.spark"  %% "spark-sql"                % Version.spark
  val sparkGraphX    	= "org.apache.spark"  %% "spark-graphx"             % Version.spark
  val sparkMlLib        = "org.apache.spark" %% "spark-mllib"             % Version.spark
  val sparkStreamKafka  = "org.apache.spark"  %% "spark-streaming-kafka"  % Version.spark
}

object Dependencies {

  import Library._

  val SparkLib = Seq(
    sparkCore 		% "provided",
    sparkStreaming 	% "provided",
    sparkSql 		% "provided",
    sparkGraphX 	% "provided",
    sparkMlLib % "provided",
    sparkStreamKafka,
    scalaTest      	% "test",
    akkaActor,
    akkaTestKit,
    logbackClassic	% "test",
    mockitoAll     	% "test"
  )
}

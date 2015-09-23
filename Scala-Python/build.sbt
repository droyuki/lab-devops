name := "Scala-Python"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

lazy val jvmOptions = Seq(
  "-Djava.library.path=./lib"
)
fork in run := true
javaOptions in run ++= jvmOptions

fork in console := true
javaOptions in console ++= jvmOptions

fork in test := true
javaOptions in Test ++= jvmOptions
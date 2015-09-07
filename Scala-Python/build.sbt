name := "Scala-Python"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

fork in run := true
javaOptions in run += "-Djava.library.path=./lib"

fork in console := true
javaOptions in console += "-Djava.library.path=./lib"

fork in test := true
javaOptions in Test += "-Djava.library.path=./lib"
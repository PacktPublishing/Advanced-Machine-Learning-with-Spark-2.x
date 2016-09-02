name := """apache-spark-2-scala-starter-template"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.postgresql" % "postgresql" % "9.4.1209.jre7",
  "log4j" % "log4j" % "1.2.17",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test)

javaOptions in (Test, run) ++= Seq("-Dspark.master=local",
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

outputStrategy := Some(StdoutOutput)

fork := true

coverageEnabled in Test:= true



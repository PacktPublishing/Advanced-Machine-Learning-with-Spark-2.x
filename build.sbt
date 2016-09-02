name := """apache-spark-2-scala-starter-template"""

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.postgresql" % "postgresql" % "9.4.1209.jre7",
  "log4j" % "log4j" % "1.2.17",
  "edu.umd" % "cloud9" % "1.5.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "com.databricks" % "spark-xml_2.10" % "0.4.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11"
)

javaOptions in(Test, run) ++= Seq("-Dspark.master=local",
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

outputStrategy := Some(StdoutOutput)

fork := true

coverageEnabled in Test := true



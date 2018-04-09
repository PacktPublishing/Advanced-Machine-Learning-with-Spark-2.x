name := "avro-dto"

seq(sbtavro.SbtAvro.avroSettings: _*)


(version in avroConfig) := "1.7.7"

(stringType in avroConfig) := "String"

libraryDependencies ++= Seq(
  "com.twitter" % "parquet-avro" % "1.6.0"
)

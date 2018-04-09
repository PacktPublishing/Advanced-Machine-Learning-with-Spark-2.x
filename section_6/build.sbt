name := "spark-deep-learning"

version := "1.0"

scalaVersion := "2.10.4"

// Scala dependencies
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.4"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.4"

// Spark dependencies
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1"

// N-Dimensional Arrays for Java dependencies
libraryDependencies += "org.nd4j" % "nd4j-x86" % "0.4-rc3.8"

libraryDependencies += "org.nd4j" % "nd4j-jcublas-7.0" % "0.4-rc3.8"

libraryDependencies += "org.nd4j" % "nd4j-api" % "0.4-rc3.8"

// Deeplearning4j dependencies
libraryDependencies += "org.deeplearning4j" % "dl4j-spark" % "0.4-rc3.8"

// Typesafe dependencies
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

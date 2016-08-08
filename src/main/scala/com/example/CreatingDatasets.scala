package com.example

import org.apache.spark.sql.SparkSession

object CreatingDatasets {

  def runCode() = {

    val spark = SparkSession.builder().appName("temp1").master("local").getOrCreate()
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.show()
    primitiveDS.map(_ + 1).collect()

    val peopleDS = spark.read.json(Config.peopleJSONPath).as[Person]
    peopleDS.show()

    spark.stop()

  }



}

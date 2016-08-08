package com.example

import org.apache.spark.sql.SparkSession

object ProgrammingGuideSQL {

  def runCode() = {

    val spark = SparkSession.builder().appName("temp1").master("local").getOrCreate()
    import spark.sqlContext.implicits._

    val df = spark.read.json(Config.peopleJSONPath)
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    spark.stop()
  }

}

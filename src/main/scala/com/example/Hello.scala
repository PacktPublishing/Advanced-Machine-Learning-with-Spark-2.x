package com.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Hello {

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val logFile = "src/main/resources/log4j.properties"
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    sc.stop()

  }

}

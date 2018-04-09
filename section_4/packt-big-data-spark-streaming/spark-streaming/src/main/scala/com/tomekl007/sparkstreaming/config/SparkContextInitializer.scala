package com.tomekl007.sparkstreaming.config

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextInitializer {
  def createSparkContext(name: String, additionalOptions: List[(String, String)]) = {
    val sc: SparkConf = basicSparkConf(name, additionalOptions)
    SparkContext.getOrCreate(sc)
  }

  def basicSparkConf(name: String, additionalOptions: List[(String, String)]): SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(name)
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.speculation", "true")
      .set("spark.kryoserializer.buffer.max.mb", "512")
      .set("spark.driver.memory", "8g")
      .set("spark.driver.cores", "4")
      .set("spark.executor.extrajavaoptions", "4g")
    additionalOptions.foreach(option => sparkConf.set(option._1, option._2))
    sparkConf
  }
}

case class SparkApplicationConfig(appName: String) extends Serializable

package com.tomekl007.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, SparkStreamingSuite, Time}

import scala.collection.mutable

class WordCountStreamingTest extends SparkStreamingSuite {
  override def appName = "word-count-test"

  test("should calculate word count of streaming job") {
    val conf = new SparkConf().setMaster(s"local[2]").setAppName("word-count-app")

    val expectedOutput: Array[(String, Int)] = Array(
      "a" -> 3,
      "b" -> 2,
      "c" -> 4
    )

    val lines = List("a a b", "a b c", "c c c")

    val sentences = mutable.Queue[RDD[String]]()

    val streamingResults = mutable.ListBuffer.empty[Array[(String, Int)]]
    val wordCounts = ssc.queueStream(sentences)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    sentences += ssc.sparkContext.makeRDD(lines)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }


  test("should window stream calculating word count per window") {
    val conf = new SparkConf().setMaster(s"local[2]").setAppName("word-count-app")

    val expectedOutput: Array[(String, Int)] = Array(
      "a" -> 2,
      "b" -> 1
    )

    val lines = List("a a b", "a b c", "c c c")

    val sentences = mutable.Queue[RDD[String]]()

    val streamingResults = mutable.ListBuffer.empty[Array[(String, Int)]]
    val wordCounts = ssc.queueStream(sentences).flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts
      .window(Seconds(1))
      .foreachRDD((rdd: RDD[(String, Int)], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    sentences += ssc.sparkContext.makeRDD(List(lines.head))
    //1 -> a a b
    //2 -> "a b c", "c c c"
    Thread.sleep(2000)
    sentences += ssc.sparkContext.makeRDD(lines.slice(1, 2))
    assertInputMatchExpected(streamingResults, expectedOutput)

  }


  test("should join two streams ") {

    val expectedOutput: Array[(Int, (String, Double))] = Array(
      1 -> ("1st Street", 10.23)
    )


    val addresses = mutable.Queue[RDD[(Int, String)]]()
    val transactions = mutable.Queue[RDD[(Int, Double)]]()

    val streamingResults = mutable.ListBuffer.empty[Array[(Int, (String, Double))]]
    val joined = ssc
      .queueStream(addresses).map { case (id, street) => (id, street) }
      .join(ssc.queueStream(transactions))

    joined.foreachRDD((rdd: RDD[(Int, (String, Double))], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    addresses += spark.makeRDD(List(1 -> "1st Street"))
    transactions += spark.makeRDD(List(1 -> 10.23, 2 -> 123.2))
    assertInputMatchExpected(streamingResults, expectedOutput)

  }
}

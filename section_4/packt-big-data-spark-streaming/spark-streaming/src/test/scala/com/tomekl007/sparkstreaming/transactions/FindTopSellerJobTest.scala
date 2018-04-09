package com.tomekl007.sparkstreaming.transactions

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{SparkStreamingSuite, Time}

import scala.collection.mutable
import scala.language.postfixOps

class FindTopSellerJobTest extends SparkStreamingSuite {

  private val underTest = new FindTopSellerJob()

  override def appName: String = this.getClass.getSimpleName


  test("should find top-sellers from a stream of transactions") {
    //given
    val input = Seq(
      Transaction(UUID.randomUUID().toString, "item_1", 10.2),
      Transaction(UUID.randomUUID().toString, "item_1", 10.2),
      Transaction(UUID.randomUUID().toString, "item_1", 10.2),
      Transaction(UUID.randomUUID().toString, "item_2", 99.2),
      Transaction(UUID.randomUUID().toString, "item_3", 12.2)
    )
    val expectedOutput: Array[TopSeller] = Array(
      TopSeller("item_1", "some_description", 10.0)
    )

    val transactions = mutable.Queue[RDD[Transaction]]()
    val streamingResults = mutable.ListBuffer.empty[Array[(TopSeller)]]
    val results = underTest.processTransactions(ssc.queueStream(transactions))
    results.foreachRDD((rdd: RDD[(TopSeller)], time: Time) => streamingResults += rdd.collect)

    ssc.start()

    //when
    transactions += spark.makeRDD(input)
    assertInputMatchExpected(streamingResults, expectedOutput)

  }


  test("should find top-sellers based on amount from a stream of transactions") {
    //given
    val input = Seq(
      Transaction(UUID.randomUUID().toString, "item_1", 10.2),
      Transaction(UUID.randomUUID().toString, "item_1", 100.2),
      Transaction(UUID.randomUUID().toString, "item_2", 99.2),
      Transaction(UUID.randomUUID().toString, "item_3", 12.2)
    )
    val expectedOutput: Array[TopSeller] = Array(
      TopSeller("item_1", "some_description", 10.0)
    )

    val transactions = mutable.Queue[RDD[Transaction]]()
    val streamingResults = mutable.ListBuffer.empty[Array[(TopSeller)]]
    val results = underTest.processTransactions(ssc.queueStream(transactions))
    results.foreachRDD((rdd: RDD[(TopSeller)], time: Time) => streamingResults += rdd.collect)

    ssc.start()

    //when
    transactions += spark.makeRDD(input)
    assertInputMatchExpected(streamingResults, expectedOutput)

  }

}

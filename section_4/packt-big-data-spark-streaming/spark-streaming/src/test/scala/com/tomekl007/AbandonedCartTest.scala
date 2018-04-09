package com.tomekl007

import com.tomekl007.sparkstreaming.abandonedCart._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{SparkStreamingSuite, Time}

import scala.collection.mutable

class AbandonedCartTest extends SparkStreamingSuite {
  override def appName = "abandoned-cart-test"

  val underTest = new AbandonedCartJob()

  test("should produce abandoned cart notification") {
    val expectedOutput: Array[AbandonedCartNotification] = Array(
      AbandonedCartNotification("1")
    )

    val events = List(AddToCart("1"))

    val cartEvents = mutable.Queue[RDD[CartEvent]]()
    val streamingResults = mutable.ListBuffer.empty[Array[AbandonedCartNotification]]

    val result = underTest.processAbandonedCart(ssc.queueStream(cartEvents))

    result.foreachRDD((rdd: RDD[AbandonedCartNotification], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    cartEvents += ssc.sparkContext.makeRDD(events)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }

  test("should NOT produce abandoned cart notification when add and remove arrived") {
    val expectedOutput: Array[AbandonedCartNotification] = Array(
    )

    val events = List(AddToCart("1"), RemoveFromCart("1"))

    val cartEvents = mutable.Queue[RDD[CartEvent]]()
    val streamingResults = mutable.ListBuffer.empty[Array[AbandonedCartNotification]]

    val result = underTest.processAbandonedCart(ssc.queueStream(cartEvents))

    result.foreachRDD((rdd: RDD[AbandonedCartNotification], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    cartEvents += ssc.sparkContext.makeRDD(events)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }


  test("should produce abandoned cart notification only for user that did not remove from cart") {
    val expectedOutput: Array[AbandonedCartNotification] = Array(
      AbandonedCartNotification("2")
    )

    val events = List(AddToCart("1"), RemoveFromCart("1"), AddToCart("2"))

    val cartEvents = mutable.Queue[RDD[CartEvent]]()
    val streamingResults = mutable.ListBuffer.empty[Array[AbandonedCartNotification]]

    val result = underTest.processAbandonedCart(ssc.queueStream(cartEvents))

    result.foreachRDD((rdd: RDD[AbandonedCartNotification], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    cartEvents += ssc.sparkContext.makeRDD(events)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }


}

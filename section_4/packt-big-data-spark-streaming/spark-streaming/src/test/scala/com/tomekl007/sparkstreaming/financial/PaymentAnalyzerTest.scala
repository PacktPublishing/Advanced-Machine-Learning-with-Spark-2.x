package com.tomekl007.sparkstreaming.financial

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{SparkStreamingSuite, Time}

import scala.collection.mutable

class PaymentAnalyzerTest extends SparkStreamingSuite {

  private val underTest = new PaymentAnalyzer()

  override def appName: String = this.getClass.getSimpleName


  test("should filter out incorrect payments") {
    //given
    val p1 = Payment("1", "2", 12.2)
    val p2 = Payment("1", "666", 23.2)
    val p3 = Payment("666", "1", 23.2)
    val p4 = Payment("1", "2", -23.2)

    val input = Seq(p1, p2, p3, p4)
    val expectedOutput: Array[PaymentValidated] = Array(
      PaymentValidated.fromPayment(p1)
    )

    val payments = mutable.Queue[RDD[Payment]]()
    val streamingResults = mutable.ListBuffer.empty[Array[(PaymentValidated)]]
    val results = underTest.processPayments(ssc.queueStream(payments))
    results.foreachRDD((rdd: RDD[(PaymentValidated)], time: Time) => streamingResults += rdd.collect)

    ssc.start()

    //when
    payments += spark.makeRDD(input)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }

}

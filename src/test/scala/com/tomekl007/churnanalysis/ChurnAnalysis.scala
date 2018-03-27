package com.tomekl007.churnanalysis

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ChurnAnalysis extends FunSuite {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()


  test("should calculate customer churn based on quantity") {
    //given
    import spark.sqlContext.implicits._
    val transactions =
      spark.sparkContext.makeRDD(List(
        Transaction(UUID.randomUUID().toString, "2017", "11", "02", 12),
        Transaction(UUID.randomUUID().toString, "2017", "11", "04", 23),
        Transaction(UUID.randomUUID().toString, "2017", "11", "07", 51),
        Transaction(UUID.randomUUID().toString, "2017", "11", "10", 51),
        Transaction(UUID.randomUUID().toString, "2017", "12", "02", 12),
        Transaction(UUID.randomUUID().toString, "2017", "12", "08", 32)
      )).toDS()
    transactions.createOrReplaceTempView("transactions")

    //when
    val transactionsInNovember = transactions
      .where("year = '2017' and month = '11'")
      .count()

    val transactionsInDecember = transactions
      .where("year = '2017' and month = '12'")
      .count()

    //then
    assert(transactionsInDecember.toDouble / transactionsInNovember.toDouble == 0.5)
  }

  test("should calculate customer churn based on transactions amount") {
    //given
    import spark.sqlContext.implicits._
    val transactions =
      spark.sparkContext.makeRDD(List(
        Transaction(UUID.randomUUID().toString, "2017", "11", "02", 12),
        Transaction(UUID.randomUUID().toString, "2017", "11", "04", 23),
        Transaction(UUID.randomUUID().toString, "2017", "11", "07", 51),
        Transaction(UUID.randomUUID().toString, "2017", "11", "10", 51),
        Transaction(UUID.randomUUID().toString, "2017", "12", "02", 12),
        Transaction(UUID.randomUUID().toString, "2017", "12", "08", 32)
      )).toDF()
    transactions.createOrReplaceTempView("transactions")

    //when
    val transactionsSumOfAmounts = transactions
      .groupBy(transactions("year"), transactions("month"))
      .sum("amount")

    val transactionsAmountsDecember
    = transactionsSumOfAmounts
      .where("year = '2017' AND month = '12'")
      .select("sum(amount)")
      .head

    val transactionAmountsNovember =
      transactionsSumOfAmounts
        .where("year = '2017' AND month = '11'")
        .select("sum(amount)")
        .head

    //then
    assert((transactionsAmountsDecember.getDecimal(0).doubleValue() /
      transactionAmountsNovember.getDecimal(0).doubleValue()) < 0.33)


  }
}

case class Transaction(
                        userId: String,
                        year: String,
                        month: String,
                        day: String,
                        amount: BigDecimal
                      )


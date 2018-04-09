package com.tomekl007.sparkstreaming.transactions


import com.tomekl007.sink.DStreamKafkaSink
import com.tomekl007.sparkstreaming._
import com.tomekl007.sparkstreaming.config.{SparkStreamingApplication, SparkStreamingApplicationConfig}
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.{Duration, SECONDS}

class FindTopSellerJob extends SparkStreamingApplication {

  override def sparkAppName: String = "top-seller"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "/tmp")

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      val stream: DStream[Transaction] = DStreamProvider.transactions(ssc)
      val sink: DStreamKafkaSink[TopSeller] = new DStreamKafkaSink()

      processStream(ssc, stream, sink)
    }
  }

  def processStream(ssc: StreamingContext,
                    stream: DStream[Transaction],
                    sink: DStreamKafkaSink[TopSeller]): Unit = {
    val result = processTransactions(stream)
    sink.write(ssc, result)
  }

  /**
    * top seller is when number of transactions in time window is >= 3
    * OR sum amount of all transactions is >= 100
    */
  def processTransactions(stream: DStream[Transaction]): DStream[TopSeller] = {
    stream
      .window(Seconds(5), Seconds(1))
      .map(t => (t.itemId, t))
      .combineByKey(
        FindTopSellerJob.createCombiner,
        FindTopSellerJob.merger,
        FindTopSellerJob.mergeCombiner,
        new HashPartitioner(8)
      ).filter { case (_, topSellingAggregate) =>
      FindTopSellerJob.isATopSeller(topSellingAggregate)
    }.map(_._1)
      .map(ItemProductEnricher.enrich)
  }

}

object FindTopSellerJob {
  private val NumberOfItemsInTimeWindowToBeTopSeller = 3
  private val SumOfAmountsToBeTopSeller = 100

  def isATopSeller(topSellingAggregate: TopSellingAggregate): Boolean = {
    if (topSellingAggregate.count >= NumberOfItemsInTimeWindowToBeTopSeller) true
    else if (topSellingAggregate.sumOfAmounts >= SumOfAmountsToBeTopSeller) true
    else false
  }


  def createCombiner(t: Transaction): TopSellingAggregate =
    TopSellingAggregate(1, t.amount)

  def merger(topSellingAggregate: TopSellingAggregate, t: Transaction): TopSellingAggregate = {
    TopSellingAggregate(
      topSellingAggregate.count + 1,
      topSellingAggregate.sumOfAmounts + t.amount
    )
  }

  def mergeCombiner(t1: TopSellingAggregate, t2: TopSellingAggregate): TopSellingAggregate =
    TopSellingAggregate(t1.count + t2.count, t1.sumOfAmounts + t2.sumOfAmounts)

  def main(args: Array[String]): Unit = {
    val job = new FindTopSellerJob()

    job.start()
  }
}

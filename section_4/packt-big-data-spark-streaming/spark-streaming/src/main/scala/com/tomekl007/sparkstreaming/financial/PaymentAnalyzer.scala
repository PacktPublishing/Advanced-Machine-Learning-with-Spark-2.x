package com.tomekl007.sparkstreaming.financial

import com.tomekl007.sink.DStreamKafkaSink
import com.tomekl007.sparkstreaming.config.{SparkStreamingApplication, SparkStreamingApplicationConfig}
import com.tomekl007.sparkstreaming.DStreamProvider
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.{Duration, SECONDS}


class PaymentAnalyzer extends SparkStreamingApplication {

  override def sparkAppName: String = "payment-analyzer"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "hdfs://payment/checkpoint")

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      val stream: DStream[Payment] = DStreamProvider.paymentProvider(ssc)
      val sink: DStreamKafkaSink[PaymentValidated] = new DStreamKafkaSink()

      processStream(ssc, stream, sink)
    }
  }

  def processStream(ssc: StreamingContext, stream: DStream[Payment],
                    sink: DStreamKafkaSink[PaymentValidated]): Unit = {
    val result = processPayments(stream)
    sink.write(ssc, result)
  }

  def processPayments(stream: DStream[Payment]): DStream[PaymentValidated] = {
    stream
      .filter(e => BlacklistUsersService.userIsNotBlacklisted(e.userId))
      .filter(e => BlacklistUsersService.userIsNotBlacklisted(e.to))
      .filter(AbandonedCartJob.amountIsProper)
      .map(PaymentValidated.fromPayment)
  }

}

object AbandonedCartJob {

  def main(args: Array[String]): Unit = {
    val job = new PaymentAnalyzer()

    job.start()
  }

  def amountIsProper: (Payment) => Boolean = {
    p => p.amount > 0
  }

}




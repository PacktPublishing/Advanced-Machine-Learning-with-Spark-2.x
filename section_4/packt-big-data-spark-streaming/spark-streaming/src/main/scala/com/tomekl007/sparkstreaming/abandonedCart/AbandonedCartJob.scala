package com.tomekl007.sparkstreaming.abandonedCart

import com.tomekl007.sink.DStreamKafkaSink
import com.tomekl007.sparkstreaming._
import com.tomekl007.sparkstreaming.config.{SparkStreamingApplication, SparkStreamingApplicationConfig}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.{Duration, SECONDS}

class AbandonedCartJob extends SparkStreamingApplication {

  override def sparkAppName: String = "abandoned-cart-job"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "file://temporary-directory}")

  //when running on the cluster the Checkpointing dir should be on hdfs

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      val stream: DStream[CartEvent] = DStreamProvider.provideCartEvents(ssc)
      val sink: DStreamKafkaSink[AbandonedCartNotification] = new DStreamKafkaSink()

      processStream(ssc, stream, sink)
    }
  }

  def processStream(ssc: StreamingContext, stream: DStream[CartEvent],
                    sink: DStreamKafkaSink[AbandonedCartNotification]): Unit = {
    val result = processAbandonedCart(stream)
    sink.write(ssc, result)
  }

  def processAbandonedCart(stream: DStream[CartEvent]): DStream[AbandonedCartNotification] = {
    stream
      .map(e => (e.userId, e))
      .groupByKey()
      .filter(AbandonedCartJob.hasOnlyAddToCart)
      .flatMap(e => e._2)
      .map(c => AbandonedCartNotification(c.userId))

  }

}

object AbandonedCartJob {

  def main(args: Array[String]): Unit = {
    val job = new AbandonedCartJob()

    job.start()
  }


  def hasOnlyAddToCart(eventsForUser: (String, Iterable[CartEvent])): Boolean = {
    eventsForUser._2
      .count(e =>
        e.isInstanceOf[AddToCart]) == eventsForUser._2.size
  }

}



package com.tomekl007.sink

import com.tomekl007.WithId
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait Sink[T <: WithId] {
  def write(ssc: StreamingContext, result: DStream[T])

}

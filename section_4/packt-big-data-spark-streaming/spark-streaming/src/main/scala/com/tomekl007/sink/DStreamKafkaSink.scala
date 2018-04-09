package com.tomekl007.sink

import java.util.Properties

import com.tomekl007.{WithId, WithUserId}
import org.apache.kafka.clients.producer._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.codehaus.jackson.map.ObjectMapper


class DStreamKafkaSink[T <: WithId] extends Sink[T] {
  def write(ssc: StreamingContext, result: DStream[T]) = {
    val properties = new Properties() //supply your real kafka config
    properties.put("bootstrap.servers", "broker1:9092,broker2:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer: Producer[String, String] = new KafkaProducer[String, String](properties)
    val objectMapper: ObjectMapper = new ObjectMapper()

    val producerVar = ssc.sparkContext.broadcast(kafkaProducer)
    val topicVar = ssc.sparkContext.broadcast("output_topic_name")
    val objectMapperVar = ssc.sparkContext.broadcast[ObjectMapper](objectMapper)

    result.foreachRDD { rdd =>
      rdd.foreach { record =>
        val topic = topicVar.value
        val producer = producerVar.value

        producer.send(
          new ProducerRecord(
            topic,
            record.id,
            objectMapperVar.value.writeValueAsString(record) //in production ready app consider using avro format
          ),
          new KafkaCallbackHandler()
        )
      }
    }
  }

}

class KafkaCallbackHandler extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}

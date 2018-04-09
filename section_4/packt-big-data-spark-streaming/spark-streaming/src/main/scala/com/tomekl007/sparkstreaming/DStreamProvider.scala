package com.tomekl007.sparkstreaming

import com.tomekl007.sparkstreaming.abandonedCart.CartEvent
import com.tomekl007.sparkstreaming.financial.Payment
import com.tomekl007.sparkstreaming.state.UserEvent
import com.tomekl007.sparkstreaming.transactions.Transaction
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.codehaus.jackson.map.ObjectMapper

object DStreamProvider {
  private val properties = Map(
    "bootstrap.servers" -> "localhost:9091", //set your prod env configuration
    "group.id" -> "payment-processor",
    "key.deserializer" ->
      "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" ->
      "org.apache.kafka.common.serialization.StringDeserializer"
  )

  def provideUserEvents(ssc: StreamingContext): DStream[UserEvent] = {
    KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, properties, Set("user_event"))
      .map(deserializeToUserEvent)

  }

  val objectMapper: ObjectMapper = new ObjectMapper()

  def providePageViews(ssc: StreamingContext): DStream[PageView] = {

    KafkaUtils.createStream[String, String, DefaultDecoder, DefaultDecoder](
      ssc,
      properties,
      Map("page_views" -> 1),
      StorageLevel.MEMORY_AND_DISK
    ).map(deserializeToPageView)

  }

  def provideCartEvents(ssc: StreamingContext): DStream[CartEvent] = {
    KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, properties, Set("cart_event"))
      .map(deserializeToCartEvent)
  }

  def paymentProvider(ssc: StreamingContext): DStream[Payment] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, properties, Set("payment"))
      .map(deserialzieToPayment)
  }


  def deserializeToPageView(tuple: (String, String)): PageView = {
    objectMapper.readValue(tuple._2, classOf[PageView]) //in prod it should be binary format, for example avro
  }

  def deserializeToUserEvent(tuple: (String, String)): UserEvent = {
    objectMapper.readValue(tuple._2, classOf[UserEvent])
  }

  def deserializeToCartEvent(tuple: (String, String)): CartEvent = {
    objectMapper.readValue(tuple._2, classOf[CartEvent])
  }

  def deserialzieToPayment(tuple: (String, String)): Payment = {
    objectMapper.readValue(tuple._2, classOf[Payment])
  }

  def transactions(ssc: StreamingContext): DStream[Transaction] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, properties, Set("transactions"))
      .map(deserializeToTransaction)
  }

  def deserializeToTransaction(tuple: (String, String)): Transaction = {
    objectMapper.readValue(tuple._2, classOf[Transaction])
  }


}

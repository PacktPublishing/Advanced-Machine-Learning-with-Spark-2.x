package com.tomekl007.sparkstreaming

import com.tomekl007.sparkstreaming.state.{MapWithStateExample, UserEvent, UserSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{SparkStreamingSuite, Time}

import scala.collection.mutable

class MapWithStateTest extends SparkStreamingSuite {

  private val underTest = new MapWithStateExample()

  override def appName: String = this.getClass.getSimpleName

  test("should create stateful stream processing and load first state from cassadnra") {
    //given
    val e1 = UserEvent(1, "click_1", false)
    val e2 = UserEvent(1, "click_2", false)
    val input = Seq(e1, e2)

    val expectedOutput: Array[(Int, UserSession)] = Array(
      (1, UserSession(List(
        UserEvent(1, "click_2", false),
        UserEvent(1, "click_1", false),
        UserEvent(1, "content", false)
      )))
    )


    val events = mutable.Queue[RDD[UserEvent]]()

    val results = underTest.processStream(
      ssc.queueStream(events))
    val streamingResults = mutable.ListBuffer.empty[Array[(Int, UserSession)]]

    results.foreachRDD((rdd: RDD[(Int, UserSession)], time: Time) => streamingResults += rdd.collect)

    ssc.start()

    //when
    events += spark.makeRDD(input)
    assertInputMatchExpected(streamingResults, expectedOutput)

  }

}

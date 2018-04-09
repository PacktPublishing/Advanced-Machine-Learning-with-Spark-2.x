package com.tomekl007.sparkstreaming.state

import com.tomekl007.sparkstreaming.DStreamProvider
import com.tomekl007.sparkstreaming.config.{SparkStreamingApplication, SparkStreamingApplicationConfig}
import com.tomekl007.sparkstreaming.state.MapWithStateExample.updateUserEvents
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}

import scala.concurrent.duration.{Duration, SECONDS}

class MapWithStateExample extends SparkStreamingApplication {

  override def sparkAppName: String = "map_with_state"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "file://temporary-directory")

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      val stream: DStream[UserEvent] = DStreamProvider.provideUserEvents(ssc)

      processStream(stream)
    }
  }

  def processStream(stream: DStream[UserEvent]): DStream[(Int, UserSession)] = {
    val stateSpec = StateSpec.function(updateUserEvents _)

    val mapped = stream
      .map(e => (e.id, e))
      .mapWithState(stateSpec)


    mapped.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreach(maybeUserSession =>
          maybeUserSession.foreach(e => CassandraLoader.connect().insertNewEvent(e)))
      }
    }
    mapped.stateSnapshots()
  }

}

object MapWithStateExample {


  def updateUserEvents(key: Int,
                       value: Option[UserEvent],
                       state: State[UserSession]): Option[UserSession] = {
    println(s"update for key: $key, value: $value, state: $state")
    /*
    Get existing user events, or if this is our first iteration
    create an empty sequence of events.
    */
    val existingEvents: Seq[UserEvent] =
      state
        .getOption()
        .map(_.userEvents)
        .getOrElse(CassandraLoader.connect().selectAllFromTimeSeries(key))

    /*
    Extract the new incoming value, appending the new event with the old
    sequence of events.
    */
    val updatedUserSession: UserSession =
      value
        .map(newEvent => UserSession(newEvent +: existingEvents))
        .getOrElse(UserSession(existingEvents))

    /*
    Look for the end event. If found, return the final `UserSession`,
    If not, update the internal state and return `None`
    */
    updatedUserSession.userEvents.find(_.isLast) match {
      case Some(_) =>
        state.remove()
        Some(updatedUserSession)
      case None =>
        state.update(updatedUserSession)
        None
    }
  }

}

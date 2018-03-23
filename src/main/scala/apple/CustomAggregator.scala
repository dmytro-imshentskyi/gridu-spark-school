package apple

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable


class CustomAggregator extends Aggregator[Event, Seq[Event], Seq[EventWithSession]] {

  override def zero: Seq[Event] = Seq.empty[Event]

  override def reduce(buf: Seq[Event], event: Event): Seq[Event] = buf :+ event

  override def merge(buf1: Seq[Event], buf2: Seq[Event]): Seq[Event] = buf2 ++ buf1

  override def finish(buf: Seq[Event]): Seq[EventWithSession]  = {
    val sortedEvents = buf.sorted
    val eventsBySession = mutable.HashMap[Long, Seq[Event]]()
    var prevEvent: Event = null
    var sessionId: Long = 0L

    for(event <- sortedEvents) {
      if(prevEvent == null) {
        eventsBySession(sessionId) = Seq(event)
        prevEvent = event
      } else {
        if(isCloseLessThanFiveMinutes(prevEvent, event)) {
          eventsBySession(sessionId) = eventsBySession(sessionId) :+ event
        } else {
          sessionId = sessionId + 1
          eventsBySession(sessionId) = Seq(event)
        }
        prevEvent = event
      }
    }

    val eventsSessionStatData = eventsBySession.map(item => {
      val sessionStartTime = item._2.minBy(_.eventTime).eventTime
      val sessionEndTime = item._2.maxBy(_.eventTime).eventTime
      val sessionId = item._2.headOption.map(_.categoryId).getOrElse(0L) * sessionEndTime.getTime
      (item._1, (sessionId, sessionStartTime, sessionEndTime))
    })

    for((sessionId, events) <- eventsBySession.toSeq; event <- events)
      yield enrichEventWithSessionData(event, eventsSessionStatData(sessionId))
  }

  private[this] def enrichEventWithSessionData(event: Event, sessionData: (Long, Timestamp, Timestamp)) = {
    EventWithSession(event, sessionData._1, sessionData._2, sessionData._3)
  }

  private[this] def isCloseLessThanFiveMinutes(event1: Event, event2: Event) =
    Math.abs(event1.eventTime.getTime - event2.eventTime.getTime) < 5 * 1000 * 60

  override def bufferEncoder = ExpressionEncoder()

  override def outputEncoder = ExpressionEncoder()
}

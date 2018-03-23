package apple

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable


class CustomAggregator extends Aggregator[Event, Seq[Event], Seq[(Long, Event)]] {

  override def zero: Seq[Event] = Seq.empty[Event]

  override def reduce(buf: Seq[Event], event: Event): Seq[Event] = buf :+ event

  override def merge(buf1: Seq[Event], buf2: Seq[Event]): Seq[Event] = {
    if(buf1.isEmpty) {
      return buf2
    }
    if(buf2.isEmpty) {
      return buf1
    }
    buf2 ++ buf1
  }

  override def finish(buf: Seq[Event]): Seq[(Long, Event)]  = {
    val sortedEvents = buf.sorted(EventOrdering)

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

    for((sessionId, events) <- eventsBySession.toSeq; event <- events) yield (sessionId, event)
  }

  private[this] def isCloseLessThanFiveMinutes(event1: Event, event2: Event): Boolean =
    Math.abs(event1.eventTime.getTime - event2.eventTime.getTime) < 5 * 1000 * 60

  override def bufferEncoder = ExpressionEncoder()

  override def outputEncoder = ExpressionEncoder()
}
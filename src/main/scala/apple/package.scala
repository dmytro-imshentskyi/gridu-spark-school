import java.sql.Timestamp

package object apple {

  case class Event(categoryId: Long, productId: Long, userId: Long,
                   title: String, eventTime: Timestamp, eventType: String)

  case object EventOrdering extends Ordering[Event] {
    override def compare(x: Event, y: Event): Int = x.eventTime.compareTo(y.eventTime)
  }

  implicit object LocalDateTimeOrdering extends Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }

  case class EventWithSession(event: Event, sessionId: Long, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

}

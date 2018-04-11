import java.sql.Timestamp

package object gridu {

  case class Event(categoryId: Long, productId: Long, userId: Long,
                   title: String, eventTime: Timestamp, eventType: String)

  case class EventWithSession(event: Event, sessionId: Long,
                              sessionStartTime: Timestamp, sessionEndTime: Timestamp)

  implicit case object LocalDateTimeOrdering extends Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
}

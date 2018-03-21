import java.sql.Timestamp

package object apple {

  case class Event(categoryId: Long, productId: Long, userId: Long,
                   title: String, eventTime: Timestamp, eventType: String)

}

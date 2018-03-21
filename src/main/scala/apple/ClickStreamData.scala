package apple

import java.sql.Timestamp

object ClickStreamData {

  val events = Seq(
    Event(1, 101, 1001, "product-101", Timestamp.valueOf("2018-03-15 16:10:01"), "Open product page"),
    Event(1, 101, 1001, "product-101", Timestamp.valueOf("2018-03-15 16:10:35"), "Open description"),
    Event(1, 101, 1001, "product-101", Timestamp.valueOf("2018-03-15 16:11:22"), "Like"),
    Event(1, 101, 1001, "product-101", Timestamp.valueOf("2018-03-15 16:12:22"), "Add to bucket"),

    Event(1, 109, 1001, "product-109", Timestamp.valueOf("2018-03-15 16:05:01"), "Open product page"),
    Event(1, 109, 1001, "product-109", Timestamp.valueOf("2018-03-15 16:05:35"), "Open description"),
    Event(1, 109, 1001, "product-109", Timestamp.valueOf("2018-03-15 16:08:32"), "Like"),
    Event(1, 109, 1001, "product-109", Timestamp.valueOf("2018-03-15 16:08:51"), "Add to bucket"),

    Event(1, 111, 1001, "product-111", Timestamp.valueOf("2018-03-15 16:00:01"), "Open product page"),
    Event(1, 111, 1001, "product-111", Timestamp.valueOf("2018-03-15 16:02:33"), "Like"),
    
    Event(1, 101, 1002, "product-101", Timestamp.valueOf("2018-03-15 16:01:32"), "Open product page"),
    Event(1, 101, 1002, "product-101", Timestamp.valueOf("2018-03-15 16:01:45"), "Open description"),
    Event(1, 101, 1002, "product-101", Timestamp.valueOf("2018-03-15 16:07:59"), "Dislike"),

    Event(1, 102, 1002, "product-1-2", Timestamp.valueOf("2018-03-15 16:01:32"), "Open product page"),
    Event(1, 102, 1002, "product-1-2", Timestamp.valueOf("2018-03-15 16:01:34"), "Dislike"),

    Event(1, 103, 1002, "product-1-3", Timestamp.valueOf("2018-03-15 16:01:59"), "Dislike"),
    Event(1, 103, 1005, "product-1-3", Timestamp.valueOf("2018-03-15 16:12:59"), "Open product page"),
    Event(1, 103, 1005, "product-1-3", Timestamp.valueOf("2018-03-15 16:14:59"), "Add to bucket"),

    Event(1, 111, 1002, "product-111", Timestamp.valueOf("2018-03-15 16:02:33"), "Open product page"),
    Event(1, 111, 1002, "product-111", Timestamp.valueOf("2018-03-15 16:05:18"), "Dislike"),

    Event(1, 121, 1001, "product-121", Timestamp.valueOf("2018-03-15 16:02:19"), "Open product page"),
    Event(1, 121, 1001, "product-121", Timestamp.valueOf("2018-03-15 16:10:03"), "Like"),

    Event(2, 104, 1002, "product-1-3", Timestamp.valueOf("2018-03-15 16:01:32"), "Open product page"),
    Event(2, 104, 1002, "product-1-3", Timestamp.valueOf("2018-03-15 16:01:52"), "Dislike"),

    Event(2, 201, 1001, "product-2-1", Timestamp.valueOf("2018-03-15 16:03:01"), "Open product page"),
    Event(2, 201, 1001, "product-2-1", Timestamp.valueOf("2018-03-15 16:03:21"), "Dislike"),

    Event(2, 202, 1002, "product-2-2", Timestamp.valueOf("2018-03-15 16:01:12"), "Open product page"),
    Event(2, 202, 1002, "product-2-2", Timestamp.valueOf("2018-03-15 16:01:22"), "Dislike"),

    Event(2, 203, 1001, "product-2-3", Timestamp.valueOf("2018-03-15 16:02:59"), "Open product page"),
    Event(2, 203, 1001, "product-2-3", Timestamp.valueOf("2018-03-15 16:03:40"), "Like"),

    Event(2, 204, 1002, "product-2-3", Timestamp.valueOf("2018-03-15 16:04:32"), "Open product page"),
    Event(2, 204, 1002, "product-2-3", Timestamp.valueOf("2018-03-15 16:04:32"), "Dislike"),

    Event(5, 301, 1001, "product-5-1", Timestamp.valueOf("2018-03-15 16:05:01"), "Open product page"),
    Event(5, 301, 1001, "product-5-1", Timestamp.valueOf("2018-03-15 16:05:21"), "Like"),

    Event(5, 302, 1001, "product-5-2", Timestamp.valueOf("2018-03-15 16:10:12"), "Open product page"),
    Event(5, 302, 1001, "product-5-2", Timestamp.valueOf("2018-03-15 16:10:22"), "Like")
  )
}

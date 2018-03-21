package apple

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object ClickStreamAnalyzer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("SparkEcommerceDemoApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val eventsDS: Dataset[Event] = spark.createDataset(spark.sparkContext.parallelize(ClickStreamData.events))
    eventsDS.createOrReplaceTempView("events")

    println("Task #1. Enrich incoming data with sessions")
    val resultSql = spark.sql(
      """
        SELECT categoryId, productId, userId, title, eventTime, eventType, sessionStartTime, sessionEndTime,
        (RANK() OVER(PARTITION BY categoryId, end ORDER BY categoryId, end)) * UNIX_TIMESTAMP(end) as sessionId,
        UNIX_TIMESTAMP(sessionEndTime) - UNIX_TIMESTAMP(sessionStartTime) as sessionDuration
        FROM (
            SELECT e.*,
            MIN(eventTime) OVER(PARTITION BY categoryId, window(eventTime, '5 minutes', '5 minutes')) as sessionStartTime,
            MAX(eventTime) OVER(PARTITION BY categoryId, window(eventTime, '5 minutes', '5 minutes')) as sessionEndTime,
            window.end
            FROM events e
        )
        ORDER BY categoryId, eventTime, sessionId
      """)

    resultSql.cache()
    resultSql.show(50, truncate = false)

    println("Task #2. For each category find median session duration")

    resultSql.createOrReplaceTempView("eventsWithSession")
    spark.sqlContext.udf.register("MEDIUM_UDF", mediumUdf)

    spark.sql(
      """
        SELECT categoryId,
        ROUND(MEDIUM_UDF(COLLECT_LIST(sessionDuration)) / 60, 2) as mediumOfSessionDurationInMinutes
        FROM eventsWithSession
        GROUP BY categoryId
        ORDER BY categoryId
      """).show(truncate = false)


    println("Task #3. For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins")

    spark.sql(
      """
        SELECT categoryId, timeGroup, COUNT(DISTINCT userId) as userCount
        FROM
        (SELECT
          categoryId, userId,
          CASE
            WHEN timeDiff < 1 THEN "less than 1 minute"
            WHEN timeDiff BETWEEN 1 AND 5 THEN "from 1 to 5 minutes"
            ELSE "more than 5 minutes"
          END as timeGroup
        FROM (
            SELECT categoryId, userId, (UNIX_TIMESTAMP(MAX(eventTime)) - UNIX_TIMESTAMP(MIN(eventTime))) / 60 as timeDiff
            FROM eventsWithSession
            GROUP BY categoryId, userId )
         )
         GROUP BY categoryId, timeGroup
         ORDER BY categoryId, timeGroup
      """).show(truncate = false)

    println("Task #4. For each category find top 10 products ranked by time spent by users on product pages")
    val eventsAndUserSession = spark.sql(
      """
        SELECT s.*,
        UNIX_TIMESTAMP(sessionEndTime) - UNIX_TIMESTAMP(sessionStartTime) as sessionDuration
        FROM (
            SELECT e.*,
            MIN(eventTime) OVER(PARTITION BY categoryId, productId, userId) as sessionStartTime,
            MAX(eventTime) OVER(PARTITION BY categoryId, productId, userId) as sessionEndTime
            FROM events e
        ) s
      """)

    eventsAndUserSession.createOrReplaceTempView("eventsAndUserSession")

    spark.sql(
      """
         SELECT categoryId, productId, rank FROM (
            SELECT categoryId, productId, sumUserTime, ROW_NUMBER() OVER(PARTITION BY categoryId ORDER BY sumUserTime DESC) as rank
            FROM (
                SELECT categoryId, productId,
                SUM(userTime) as sumUserTime
                FROM (
                    SELECT categoryId, productId, userId,
                    UNIX_TIMESTAMP(MAX(eventTime)) - UNIX_TIMESTAMP(MIN(eventTime)) as userTime
                    FROM eventsAndUserSession
                    GROUP BY categoryId, productId, userId
                )
                GROUP BY categoryId, productId
            )
         )
         WHERE rank <= 10
         ORDER BY categoryId, rank, productId
      """).show(truncate = false)

    spark.stop()
  }

  def medium(elements: mutable.WrappedArray[Long]): Long = {
    val size = elements.size
    if(size == 1){
      elements.head
    }

    val sortedSeq = elements.sortWith(_ < _)
    if (size % 2 == 1) sortedSeq(size / 2)
    else {
      val (up, down) = sortedSeq.splitAt(size / 2)
      (up.last + down.head) / 2
    }
  }

  def mediumUdf: UserDefinedFunction = udf[Long, mutable.WrappedArray[Long]](medium)

}
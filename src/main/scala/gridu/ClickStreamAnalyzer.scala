package gridu

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object ClickStreamAnalyzer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("SparkEcommerceDemoApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val eventsDS: Dataset[Event] = spark.createDataset(spark.sparkContext.parallelize(ClickStreamData.events))
    eventsDS.createOrReplaceTempView("events")

    println("Task #0. Enrich incoming data with session data with using custom aggregator")
    val aggregator = ClickStreamCustomAggregator.toColumn

    val eventsByAggregator = eventsDS.groupByKey(_.categoryId)
      .agg(aggregator.name("events")).select(explode($"events").as("eventsWithSession"))
    eventsByAggregator.createOrReplaceTempView("eventsWithSessionByAgr")

    spark.sql(
      """
        SELECT eventsWithSession.event.categoryId,
        eventsWithSession.event.productId,
        eventsWithSession.event.userId,
        eventsWithSession.event.title,
        eventsWithSession.event.eventType,
        eventsWithSession.event.eventTime,
        eventsWithSession.sessionStartTime,
        eventsWithSession.sessionEndTime,
        eventsWithSession.sessionId
        FROM eventsWithSessionByAgr
        ORDER BY categoryId, eventTime, sessionId
      """).show(50, truncate = false)

    println("Task #1. Enrich incoming data with session data with using sql window function")
    spark.sqlContext.udf.register("TRUNCATE", truncateUdf)

    val eventsWithSession = spark.sql(
      """
         SELECT categoryId, productId, userId, eventTime, title,
         MIN(eventTime) OVER (PARTITION BY sessionId) as sessionStartTime,
         MAX(eventTime) OVER (PARTITION BY sessionId) as sessionEndTime, sessionId
         FROM (
            SELECT de.*,
            SUM(CASE
                  WHEN
                    categoryId != prevCategoryId THEN 1
                  WHEN
                    eventTimeDiffClassifier != prevEventTimeDiffClassifier AND prevEventTimeDiffClassifier = 0 THEN 1
                  ELSE 0
                END) OVER(ORDER BY categoryId, eventTime) as sessionId
            FROM (
                SELECT ne.*,
                LAG(eventTimeDiffClassifier, 1, eventTimeDiffClassifier) OVER(ORDER BY categoryId, eventTime) as prevEventTimeDiffClassifier,
                LAG(categoryId, 1, categoryId) OVER(ORDER BY categoryId, eventTime) as prevCategoryId
                FROM (
                  SELECT xe.*
                  FROM (
                SELECT se.*,
                TRUNCATE((UNIX_TIMESTAMP(eventTime) - UNIX_TIMESTAMP(laggedEventTime)) / 60 / 5) as eventTimeDiffClassifier
                FROM (
                  SELECT e.*,
                  LAG(eventTime, 1, eventTime) OVER(PARTITION BY categoryId ORDER BY eventTime) as laggedEventTime
                  FROM events e
                ) se
              ) xe
            ) ne
          ) de
          )
          ORDER BY categoryId, eventTime
      """)

    eventsWithSession.show(50, truncate = false)
    eventsWithSession.createOrReplaceTempView("eventsWithSession")

    println("Task #2. For each category find median session duration")
    spark.sql(
      """
        SELECT categoryId, percentile_approx(sessionDuration, 0.5) as medianSessionDuration
        FROM (
          SELECT categoryId, UNIX_TIMESTAMP(sessionEndTime) - UNIX_TIMESTAMP(sessionStartTime) as sessionDuration
          FROM eventsWithSession
        )
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

  def truncate(value: Double): Int = {
    value.toInt
  }

  def truncateUdf: UserDefinedFunction = udf[Int, Double](truncate)
}
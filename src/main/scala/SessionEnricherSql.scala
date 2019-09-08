import java.sql.Timestamp
import Utils.{writeToFile, writeToFileDF}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SessionEnricherSql {

  case class Event(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String)

  case class SessionBuffer(events: List[Event], sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

  case class EventSession(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String,
                          sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

  case class EventSessionDur(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String,
                             sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    import spark.implicits._
    val ctx = spark.sqlContext
    val file = SessionEnricherSql.getClass.getResource(Config.inputFile).getFile
    val debug = Config.debug

    val df = ctx
      .read
      .option("header", value = true)
      .csv(file)
      .as[Event]

    val events = eventWithSession(spark, df, "output/task1_sql")(debug = debug)
      .withColumn("sessionDuration", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime") + 1)

    medianSessionDuration(spark, events, "output/task2_median")(debug = debug)
    userSpentTimeStat(spark, events, "output/task2_time_spent_stat")(debug = debug)

    val eventsProductSession = eventWithProductSession(spark, df, "output/task2_sql")(debug = debug)
    topNProducts(spark, eventsProductSession, "output/task2_top10")(n = Config.topN, debug = debug)
  }

  def topNProducts(spark: SparkSession, events: Dataset[EventSession], outputFolder: String)(implicit n: Int = 10, debug: Boolean = false): DataFrame = {
    import spark.implicits._
    val eventsWithDuration =
      events.withColumn("sessionDuration", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime") + 1)
        .groupBy("product", "category")
        .agg(sum("sessionDuration").as("sessionDuration"))

    val topN = eventsWithDuration
      .select(
        $"product",
        $"category",
        $"sessionDuration",
        dense_rank().over(Window.partitionBy($"category").orderBy($"sessionDuration".desc)).as("rank"))
      .where($"rank" <= n)
      .drop($"rank")

    if (debug) {
      topN.show(50)
    } else {
      writeToFileDF(outputFolder, topN)
    }

    topN
  }

  def medianSessionDuration(spark: SparkSession, events: DataFrame, outputFolder: String)(implicit debug: Boolean = false): DataFrame = {
    events
      .groupBy("category", "sessionId")
      .agg(sum("sessionDuration").as("sessionDuration"))
      .createOrReplaceTempView("events")

    val median = spark.sql(
      """
        | select category, percentile_approx(sessionDuration, 0.5) as median from events group by category
      """.stripMargin).toDF()

    if (debug) {
      median.show(50)
    } else {
      writeToFileDF(outputFolder, median)
    }

    median
  }

  def userSpentTimeStat(spark: SparkSession, events: DataFrame, outputFolder: String)(implicit debug: Boolean = false): Unit = {
    import spark.implicits._

    val userEventDurations = events
      .groupBy("category", "userId")
      .agg(sum("sessionDuration").as("sessionDuration"))

    val stat = userEventDurations
      .withColumn("less_1_min", when($"sessionDuration" < 60, $"userId"))
      .withColumn("from_1_to_1_5_min", when($"sessionDuration" >= 60 && $"sessionDuration" < 90, $"userId"))
      .withColumn("more_5_min", when($"sessionDuration" >= 90, $"userId"))
      .groupBy("category")
      .agg(
        countDistinct("less_1_min").as("less_1_min"),
        countDistinct("from_1_to_1_5_min").as("from_1_to_1_5_min"),
        countDistinct("more_5_min").as("more_5_min")
      )

    if (debug) {
      stat.show(50)
    } else {
      writeToFileDF(outputFolder, stat)
    }
  }

  def eventWithProductSession(spark: SparkSession, df: Dataset[Event], outputFolder: String)(implicit debug: Boolean = false): Dataset[EventSession] = {
    import spark.implicits._

    val wCat = Window.partitionBy($"userId").orderBy($"eventTime")

    val eventsWithSessionIds = df
      .withColumn("prev", lag($"product", 1).over(wCat))
      .withColumn("isNewSession", when($"product" === $"prev", 0).otherwise(1))
      .withColumn("sessionId", concat_ws("_", $"userId", sum($"isNewSession").over(wCat)))
      .drop("isNewSession", "prev")

    val sessions = eventsWithSessionIds
      .groupBy("sessionId")
      .agg(
        min("eventTime").as("sessionStartTime"),
        max("eventTime").as("sessionEndTime"))

    val res = eventsWithSessionIds
      .join(sessions, Seq("sessionId"))
      .as[EventSession]

    if (debug) {
      res.show(50)
    } else {
      writeToFile(outputFolder, res)
    }

    res
  }

  def eventWithSession(spark: SparkSession, df: Dataset[Event], outputFolder: String)(implicit debug: Boolean = false): Dataset[EventSession] = {
    import spark.implicits._

    val wCat = Window.partitionBy($"userId", $"category").orderBy($"eventTime")
    val wGlobal = Window.orderBy($"userId", $"eventTime")

    val eventsWithSessionIds = df
      // eventTime of the previous event
      .withColumn("prev", lag($"eventTime", 1).over(wCat))
      // if difference between current and previous eventTime is less than 5 minutes, it's still the same session
      .withColumn("isNewSession", when((unix_timestamp($"eventTime") - unix_timestamp($"prev")) <= Config.session, 0).otherwise(1))
      .withColumn("sessionId", sum($"isNewSession").over(wGlobal))
      .drop("isNewSession", "prev")

    val sessions = eventsWithSessionIds
      .groupBy("userId", "category", "sessionId")
      .agg(
        min("eventTime").as("sessionStartTime"),
        max("eventTime").as("sessionEndTime"))

    val res = eventsWithSessionIds
      .join(sessions, Seq("userId", "category", "sessionId"))
      .as[EventSession]

    if (debug) {
      res.show(50)
    } else {
      writeToFile(outputFolder, res)
    }

    res
  }


}
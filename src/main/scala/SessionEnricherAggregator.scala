import java.sql.Timestamp
import java.util.UUID

import Utils.{minDates, maxDates}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SessionEnricherAggregator {

  case class Event(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String)

  case class SessionBuffer(events: List[Event], sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

  case class EventSession(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String,
                          sessionId: String, sessionStartTime: Timestamp, sessionEndTime: Timestamp)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    import spark.implicits._
    val ctx = spark.sqlContext
    val file = SessionEnricherAggregator.getClass.getResource(Config.inputFile).getFile

    val df = ctx
      .read
      .option("header", value = true)
      .csv(file)
      .as[Event]

    task1Aggregator(spark, df, "output/task1_aggregator")(debug = Config.debug)
  }

  def task1Aggregator(spark: SparkSession, df: Dataset[Event], outputFolder: String)(implicit debug: Boolean = false): Unit = {
    import spark.implicits._

    val aggregator = new SessionAggregator()
    val res = df.groupByKey(x => (x.userId, x.category))
      .agg(aggregator.toColumn)
      .flatMap(_._2)

    if (debug) {
      res.show(50)
    } else {
      res
        .repartition(1) // just to get all records in one file, should be removed for better performance
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .mode(SaveMode.Overwrite)
        .csv(outputFolder)
    }
  }

  class SessionAggregator extends Aggregator[Event, List[SessionBuffer], List[EventSession]] {
    override def zero: List[SessionBuffer] = List.empty

    override def reduce(b: List[SessionBuffer], a: Event): List[SessionBuffer] = {
      val time = a.eventTime

      if (b.isEmpty) {
        List(SessionBuffer(List(a), UUID.randomUUID().toString, time, time))
      } else {
        val range = b.partition(buffer => {
          val start = buffer.sessionStartTime.toLocalDateTime.minusMinutes(Config.session)
          val end = buffer.sessionEndTime.toLocalDateTime.plusMinutes(Config.session)
          val t = time.toLocalDateTime
          t.isAfter(start) && t.isBefore(end) || (t.isEqual(start) || t.isEqual(end))
        })

        val inRange = range._1

        if (inRange.isEmpty) {
          SessionBuffer(List(a), UUID.randomUUID().toString, time, time) :: b
        } else {
          val head = inRange.head

          val newBuffer = head.copy(
            events = a :: head.events,
            sessionStartTime = minDates(head.sessionStartTime, time),
            sessionEndTime = maxDates(head.sessionEndTime, time))

          newBuffer :: inRange.tail ::: range._2
        }
      }
    }

    def mergeRanges(buffers: List[SessionBuffer]): List[SessionBuffer] = {
      val sorted = buffers.sortWith((x, y) => {
        val xEnd = x.sessionEndTime.toLocalDateTime
        val yStart = y.sessionStartTime.toLocalDateTime
        xEnd.isBefore(yStart) || xEnd.isEqual(yStart)
      })

      def closeEnough(x: SessionBuffer, y: SessionBuffer): Boolean = {
        val end = x.sessionEndTime.toLocalDateTime.plusMinutes(Config.session)
        val start = y.sessionStartTime.toLocalDateTime
        end.isAfter(start) || end.isEqual(start)
      }

      def process(in: List[SessionBuffer], acc: List[SessionBuffer]): List[SessionBuffer] = in match {
        case x :: y :: ys if closeEnough(x, y) => process(
          SessionBuffer(
            x.events ++ y.events,
            x.sessionId,
            minDates(x.sessionStartTime, y.sessionStartTime),
            maxDates(x.sessionStartTime, y.sessionStartTime)) :: ys, acc)
        case x :: xs => process(xs, x :: acc)
        case Nil => acc
      }

      process(sorted, Nil)
    }

    override def merge(b1: List[SessionBuffer], b2: List[SessionBuffer]): List[SessionBuffer] = {
      mergeRanges(b1 ++ b2)
    }

    override def finish(reduction: List[SessionBuffer]): List[EventSession] = {
      reduction.flatMap(x =>
        x.events.map(y => EventSession(
          y.category,
          y.product,
          y.userId,
          y.eventTime,
          y.eventType,
          x.sessionId,
          x.sessionStartTime,
          x.sessionEndTime)))
    }

    override def bufferEncoder: Encoder[List[SessionBuffer]] = Encoders.product[List[SessionBuffer]]

    override def outputEncoder: Encoder[List[EventSession]] = Encoders.product[List[EventSession]]
  }

}
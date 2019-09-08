import java.sql.Timestamp

import SessionEnricherSql.EventSession
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object Utils {

  def writeToFile(outputFolder: String, res: Dataset[EventSession]): Unit = {
    res
      .repartition(1) // just to get all records in one file, should be removed for better performance
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .csv(outputFolder)
  }

  def writeToFileDF(outputFolder: String, res: DataFrame): Unit = {
    res
      .repartition(1) // just to get all records in one file, should be removed for better performance
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .csv(outputFolder)
  }

  def minDates(a: Timestamp, b: Timestamp): Timestamp = {
    if (a.toLocalDateTime.isBefore(b.toLocalDateTime)) a else b
  }

  def maxDates(a: Timestamp, b: Timestamp): Timestamp = {
    if (a.toLocalDateTime.isAfter(b.toLocalDateTime)) a else b
  }
}

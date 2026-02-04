import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object DataSilverTable extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "bronze.data"
    val targetTable     = args(2) // e.g., "silver.data"

    logInfo(s"Starting DataSilverTable job with date: $dateToProcess, input: $inputTable, target: $targetTable")

    val spark = SparkSession.builder()
      .appName(s"Populate Silver data Table for: $dateToProcess")
      .getOrCreate()

    logInfo("SparkSession created successfully")

    // 0. Create Namespace if not exists
    val namespace = "silver"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
    logInfo(s"Namespace '$namespace' ensured")

    // 1. Read bronze data for the specified date
    val df: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))
    logInfo(s"Read ${df.count()} records from $inputTable for date $dateToProcess")

    // 2. Perform transformations to derive new columns
    val resultDf = df
      .withColumn("session_date", to_date(col("timestamp")))
      .withColumn("session_hour", hour(col("timestamp")))
    logInfo("Applied transformations: session_date and session_hour derived")

    resultDf.show(5)

    // Select columns to match target schema
    val finalDf = resultDf.select(
      col("timestamp"),
      col("session_id"),
      col("session_date"),
      col("session_hour"),
      col("msisdn"),
      col("apn"),
      col("session_duration_seconds"),
      col("bytes_uploaded"),
      col("bytes_downloaded"),
      col("session_end_reason"),
      col("cell_id"),
      col("region"),
      col("charging_amount")
    )
    logInfo("Selected target columns")

    // 3. Validation and Transformation
    val enrichedDf = finalDf.withColumn("ingestion_date", current_date())
    logInfo(s"Added ingestion_date column. Final row count: ${enrichedDf.count()}")
    
    // 4. Write to Silver Iceberg Table
    enrichedDf.write
      .mode(SaveMode.Append)
      .saveAsTable(targetTable)
    logInfo(s"Successfully wrote data to $targetTable")

    spark.stop()
    logInfo("SparkSession stopped. Job completed successfully")
  }
}
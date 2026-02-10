import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object SmsSilverTable extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "bronze.sms"
    val targetTable     = args(2) // e.g., "silver.sms"

    logInfo(s"Starting SMS Silver Table population for date: $dateToProcess")
    logInfo(s"Input table: $inputTable, Target table: $targetTable")

    val spark = SparkSession.builder()
      .appName(s"Populate Silver SMS Table for: $dateToProcess")
      .getOrCreate()

    // 0. Create Namespace if not exists
    val namespace = "silver"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
    logInfo(s"Namespace '$namespace' created or already exists")

    // 1. Read bronze data for the specified date
    val df: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))
    logInfo(s"Read ${df.count()} records from $inputTable for date $dateToProcess")

    // 2. Perform transformations to derive new columns
    val resultDf = df
      .withColumn("sms_date", to_date(col("timestamp")))
      .withColumn("sms_hour", hour(col("timestamp")))
    logInfo("Applied date and hour transformations")

    resultDf.show(5)

    // Select columns to match target schema
    val finalDf = resultDf.select(
      col("timestamp"),
      col("sms_date"),
      col("sms_hour"),
      col("sms_id"),
      col("sender_msisdn"),
      col("receiver_msisdn"),
      col("sms_type"),
      col("message_length"),
      col("delivery_status"),
      col("cell_id"),
      col("region"),
      col("charging_amount")
    )
    logInfo("Selected final columns")

    // 3. Validation and Transformation
    val enrichedDf = finalDf.withColumn("ingestion_date", current_date())
    logInfo("Added ingestion_date column")
    
    // 4. Write to Silver Iceberg Table
    enrichedDf.write
      .mode(SaveMode.Append)
      .saveAsTable(targetTable)
    logInfo(s"Successfully wrote ${enrichedDf.count()} records to $targetTable")

    logInfo("SMS Silver Table population completed successfully")
    spark.stop()
  }
}
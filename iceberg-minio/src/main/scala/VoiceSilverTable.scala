import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object VoiceSilverTable extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "bronze.voice"
    val targetTable     = args(2) // e.g., "silver.voice"

    logInfo(s"Starting VoiceSilverTable job with dateToProcess=$dateToProcess, inputTable=$inputTable, targetTable=$targetTable")

    val spark = SparkSession.builder()
      .appName(s"Populate Silver Voice Table for: $dateToProcess")
      .getOrCreate()

    logInfo("SparkSession created successfully")

    // 0. Create Namespace if not exists
    val namespace = "silver"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
    logInfo(s"Namespace '$namespace' ensured")

    // 1. Read bronze data for the specified date
    logInfo(s"Reading data from table: $inputTable for date: $dateToProcess")
    val voicedf: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))
    
    val bronzeCount = voicedf.count()
    logInfo(s"Read $bronzeCount records from bronze table")

    // 2. Perform transformations to derive new columns
    logInfo("Applying transformations...")
    val resultDf = voicedf
      .withColumn("call_date", to_date(col("timestamp")))
      .withColumn("call_hour", hour(col("timestamp")))
      .withColumn("duration_minutes", col("duration_seconds") / 60.0)
      .withColumn("call_status", 
        when(col("termination_reason").isin("NORMAL", "USER_TERMINATED"), "SUCCESS")
        .otherwise("FAILED")
      )

    resultDf.show()
    logInfo("Transformations completed")

    // Select columns to match target schema
    val finalDf = resultDf.select(
      col("timestamp"),
      col("call_date"),
      col("call_hour"),
      col("call_id"),
      col("caller_msisdn"),
      col("callee_msisdn"),
      col("call_type"),
      col("duration_seconds"),
      col("duration_minutes"),
      col("termination_reason"),
      col("call_status"),
      col("region"),
      col("cell_id"),
      col("charging_amount")
    )

    // 3. Validation and Transformation
    val enrichedDf = finalDf.withColumn("ingestion_date", current_date())
    
    val finalCount = enrichedDf.count()
    logInfo(s"Final record count before write: $finalCount")
    
    // 4. Write to Silver Iceberg Table
    logInfo(s"Writing $finalCount records to table: $targetTable")
    enrichedDf.write
      .mode(SaveMode.Append)
      .saveAsTable(targetTable)
    
    logInfo(s"Successfully wrote records to $targetTable")

    spark.stop()
    logInfo("Job completed successfully")
  }
}
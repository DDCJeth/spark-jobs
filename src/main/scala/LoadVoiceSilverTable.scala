import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object LoadVoiceSilverTable extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "bronze.voice"
    val targetTable     = args(2) // e.g., "silver.voice"


    val spark = SparkSession.builder()
      .appName(s"Populate Silver Voice Table for: $dateToProcess")
      .getOrCreate()


    // 0. Create Namespace if not exists
    val namespace = "silver"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")


    // 1. Read bronze data for the specified date
    val voicedf: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))


    // 2. Perform transformations to derive new columns
    val resultDf = voicedf
      // 1. Extract date from timestamp (equivalent to dt.date)
      .withColumn("call_date", to_date(col("timestamp")))
      
      // 2. Extract hour from timestamp (equivalent to dt.hour)
      .withColumn("call_hour", hour(col("timestamp")))
      
      // 3. Compute duration in minutes
      // Note: We divide by 60.0 to ensure floating point division
      .withColumn("duration_minutes", col("duration_seconds") / 60.0)
      
      // 4. Derive call status (equivalent to np.where)
      .withColumn("call_status", 
        when(col("termination_reason").isin("NORMAL", "USER_TERMINATED"), "SUCCESS")
        .otherwise("FAILED")
      )

    // View the result
    resultDf.show()

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
    
    // 4. Write to Silver Iceberg Table
    enrichedDf.write
      .mode(SaveMode.Append)
      .saveAsTable(targetTable)

    spark.stop()
  }
}
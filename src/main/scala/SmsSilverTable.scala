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


    val spark = SparkSession.builder()
      .appName(s"Populate Silver SMS Table for: $dateToProcess")
      .getOrCreate()


    // 0. Create Namespace if not exists
    val namespace = "silver"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")


    // 1. Read bronze data for the specified date
    val df: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))


    // 2. Perform transformations to derive new columns
    val resultDf = df
      // 1. Extract date from timestamp (equivalent to dt.date)
      .withColumn("sms_date", to_date(col("timestamp")))
      
      // 2. Extract hour from timestamp (equivalent to dt.hour)
      .withColumn("sms_hour", hour(col("timestamp")))
            

    // View the result
    resultDf.show(5)

    // Select columns to match target schema
    val finalDf = resultDf.select(
      col("timestamp"),
      col("sms_date"),
      col("sms_hour"),
      col("sms_id"),
      col("sms_msisdn"),
      col("receiver_msisdn"),
      col("sms_type"),
      col("message_length"),
      col("delivery_status"),
      col("cell_id"),
      col("region"),
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
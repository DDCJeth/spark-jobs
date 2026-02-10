import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object SmsGoldTables extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "silver.sms"

    val firstTargetTable     = "gold.sms_daily_global_kpis"
    val secondTargetTable    = "gold.sms_daily_tower_kpis"

    // Log progress: creating Spark session
    logInfo("Creating spark session ")
    val spark = SparkSession.builder()
      .appName(s"Populate Gold SMS Table for: $dateToProcess")
      .getOrCreate()
    logInfo("Spark session created successfully ")

    // 0. Create Namespace if not exists
    val namespace = "gold"
    // Log progress: creating namespace if needed
    logInfo(s"Creating Namespace if not exists: $namespace")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
    logInfo(s"Namespace created or already exists: $namespace")

    // 1. Read bronze data for the specified date
    // Log progress: reading input table for date
    logInfo(s"Reading input table: $inputTable for ingestion_date: $dateToProcess")
    val df: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))


  logInfo(s"Input data read successfully for date: $dateToProcess")

    // 2. Compute global KPIs
    // Log progress: computing global KPIs
  logInfo(s"Computing global SMS KPIs for date: $dateToProcess")
  val smsKpis = df.groupBy("sms_date")
    .agg(
      count("sms_id").as("total_sms"),
      count(when(col("delivery_status") === "DELIVERED", col("sms_id"))).as("total_delivered_sms"),
      count(when(col("delivery_status") === "FAILED", col("sms_id"))).as("total_failed_sms"),
      sum("charging_amount").as("total_revenue"),
      countDistinct("sender_msisdn").as("total_subscribers_sending_sms"),
      countDistinct("receiver_msisdn").as("total_subscribers_receiving_sms")
    )

    smsKpis.show(5)
    // Log progress: finished computing global KPIs
    logInfo(s"Computed global SMS KPIs for date: $dateToProcess")

    // Write to to Gold Table : smsKpis
    logInfo(s"Writing aggregated global KPIs to Iceberg table: $firstTargetTable")
    smsKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(firstTargetTable)
    logInfo(s"Data successfully written to Iceberg table: $firstTargetTable")


  

    val smsTowerKpis = df.groupBy("sms_date", "sms_hour", "cell_id")
      .agg(
        count("sms_id").as("total_sms"),
        count(when(col("delivery_status") === "DELIVERED", 1)).as("total_delivered_sms"),
        count(when(col("delivery_status") === "FAILED", 1)).as("total_failed_sms"),
        sum("charging_amount").as("total_revenue"),
        countDistinct("sender_msisdn").as("total_subscribers_sending_sms"),
        countDistinct("receiver_msisdn").as("total_subscribers_receiving_sms")
      )

    smsTowerKpis.show(5)
    // Log progress: finished computing tower-level KPIs
    logInfo(s"Computed tower-level KPIs for date: $dateToProcess")

    //  Write to Tower Gold Iceberg Table
    logInfo(s"Writing tower KPIs to Iceberg table: $secondTargetTable")
    smsTowerKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(secondTargetTable)
    logInfo(s"Data successfully written to Iceberg table: $secondTargetTable")

    logInfo("Stopping spark session")
    spark.stop()
  }
}
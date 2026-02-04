import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object DataGoldTables extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "silver.data"

    val firstTargetTable     = "gold.data_daily_global_kpis"
    val secondTargetTable    = "gold.data_daily_tower_kpis"

    // Log progress: creating Spark session
    logInfo("Creating spark session ")
    val spark = SparkSession.builder()
      .appName(s"Populate Gold Data Table for: $dateToProcess")
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

    // Log progress: finished reading input data
    logInfo(s"Successfully read input table: $inputTable for ingestion_date: $dateToProcess")

    val dataKpis = df.groupBy("session_date")
      .agg(
        count("session_id").as("total_sessions"),
        count(when(col("session_end_reason") === "NORMAL", col("session_id"))).as("total_active_sessions"),
        avg("session_duration_seconds").as("average_session_duration_sec"),
        avg(when(col("session_end_reason") === "USER_TERMINATED", col("session_duration_seconds"))).as("average_session_duration_terminated_sec"),
        sum("bytes_uploaded").as("total_bytes_uploaded"),
        sum("bytes_downloaded").as("total_bytes_downloaded"),
        sum("charging_amount").as("total_revenue")
      )
      .withColumn("average_session_duration_minutes", col("average_session_duration_sec") / 60)
      .withColumn("total_bytes", col("total_bytes_uploaded") + col("total_bytes_downloaded"))
      .withColumn("total_data_volume_GB", col("total_bytes") / pow(lit(1024), 3))

      // Calculating average throughput: (Total Bytes / Total Sessions) / Avg Duration
      .withColumn("average_throughput_per_session", 
        (col("total_bytes") / col("total_sessions")) / col("average_session_duration_sec")
      )

    dataKpis.show(5)
    // Log progress: finished computing global KPIs
    logInfo(s"Computed global  KPIs for date: $dateToProcess")

    // Write to to Gold Table : Kpis
    logInfo(s"Writing aggregated global KPIs to Iceberg table: $firstTargetTable")
    dataKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(firstTargetTable)
    logInfo(s"Data successfully written to Iceberg table: $firstTargetTable")



    val dataTowerKpis = df.groupBy("data_date", "data_hour", "cell_id")
      .agg(
        count("session_id").as("total_sessions"),
        count(when(col("session_end_reason") === "NORMAL", 1)).as("total_active_sessions"),
        sum("bytes_uploaded").as("total_bytes_uploaded"),
        sum("bytes_downloaded").as("total_bytes_downloaded"),
        sum("charging_amount").as("total_revenue")
      )
      // Derive total bytes and volume in GB
      .withColumn("total_bytes", col("total_bytes_uploaded") + col("total_bytes_downloaded"))
      .withColumn("total_data_volume_GB", col("total_bytes") / pow(lit(1024), 3))

    // Show 
    dataTowerKpis.show(5)
    
    logInfo(s"Computed tower-level KPIs for date: $dateToProcess")

    //  Write to Tower Gold Iceberg Table
    logInfo(s"Writing tower KPIs to Iceberg table: $secondTargetTable")
    
    dataTowerKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(secondTargetTable)
    
    logInfo(s"Data successfully written to Iceberg table: $secondTargetTable")

    logInfo("Stopping spark session")
    spark.stop()
  }
}
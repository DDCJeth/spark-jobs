import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object VoiceGoldTables extends Logging {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "silver.voice"

    val firstTargetTable     = "gold.voice_daily_global_kpis"
    val secondTargetTable    = "gold.voice_daily_tower_kpis"

    // Log progress: creating Spark session
    logInfo("Creating spark session ")
    val spark = SparkSession.builder()
      .appName(s"Populate Gold Voice Table for: $dateToProcess")
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
    val voicedf: DataFrame = spark.table(inputTable)
      .filter(col("ingestion_date") === lit(dateToProcess))

    val voiceKpis = voicedf
      .groupBy("call_date")
      .agg(
        // Simple Counts and Sums
        count("call_id").as("total_number_calls"),
        sum("duration_seconds").as("total_call_duration"),
        avg("duration_seconds").as("average_call_duration"),
        
        // Conditional Aggregation (for average success duration)
        avg(when(col("call_status") === "SUCCESS", col("duration_seconds"))).as("average_call_duration_success"),
        
        // Conditional Counts (Sum of booleans)
        sum(when(col("call_status") === "SUCCESS", 1).otherwise(0)).as("total_call_success"),
        
        // Equivalent to: (g['call_status'] == 'FAILED').sum()
        sum(when(col("call_status") === "FAILED", 1).otherwise(0)).as("total_call_failed"),
        
        // Other Sums
        sum("charging_amount").as("total_revenue"),
        sum("duration_minutes").as("total_duration_of_minutes")
      )

    voiceKpis.show(5)
    // Log progress: finished computing global KPIs
    logInfo(s"Computed global voice KPIs for date: $dateToProcess")

    // Write to to Gold Table : voiceKpis
    logInfo(s"Writing aggregated global KPIs to Iceberg table: $firstTargetTable")
    voiceKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(firstTargetTable)
    logInfo(s"Data successfully written to Iceberg table: $firstTargetTable")


    // Assuming 'voiceDf' is your DataFrame
    val voiceTowerKpis = voicedf
      .groupBy("call_date", "call_hour", "cell_id") // Group by the 3 columns
      .agg(
        // g['call_id'].count()
        count("call_id").as("total_number_calls"),
        
        // g['duration_seconds'].sum()
        sum("duration_seconds").as("total_call_duration"),
        
        // (g['call_status'] == 'SUCCESS').sum()
        sum(when(col("call_status") === "SUCCESS", 1).otherwise(0)).as("total_call_success"),
        
        // (g['call_status'] == 'FAILED').sum()
        sum(when(col("call_status") === "FAILED", 1).otherwise(0)).as("total_call_failed"),
        
        // g['charging_amount'].sum()
        sum("charging_amount").as("total_revenue")
      )

    // Show 
    voiceTowerKpis.show(5)
    logInfo(s"Computed tower-level KPIs for date: $dateToProcess")

    //  Write to Tower Gold Iceberg Table
    logInfo(s"Writing tower KPIs to Iceberg table: $secondTargetTable")
    voiceTowerKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(secondTargetTable)
    logInfo(s"Data successfully written to Iceberg table: $secondTargetTable")

    logInfo("Stopping spark session")
    spark.stop()
  }
}
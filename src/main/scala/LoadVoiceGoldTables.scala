import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import scala.io.Source

object LoadVoiceGoldTables {

  def main(args: Array[String]): Unit = {

    val dateToProcess    = args(0) // e.g., "2024-01-15"
    val inputTable = args(1) // e.g., "silver.voice"

    val firstTargetTable     = "gold.voice_daily_global_kpis"
    val secondTargetTable    = "gold.voice_daily_tower_kpis"

    val spark = SparkSession.builder()
      .appName(s"Populate Gold Voice Table for: $dateToProcess")
      .getOrCreate()


    // 0. Create Namespace if not exists
    val namespace = "gold"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")


    // 1. Read bronze data for the specified date
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

    // Write to to Gold Table : voiceKpis
    voiceKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(firstTargetTable)


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


    //  Write to Tower Gold Iceberg Table
    voiceTowerKpis.write
      .mode(SaveMode.Append)
      .saveAsTable(secondTargetTable)

    spark.stop()
  }
}
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

        // count of unique caller_msisdn
        countDistinct("caller_msisdn").as("unique_calling_subscribers"),

        // count of unique callee_msisdn
        countDistinct("callee_msisdn").as("unique_receiving_subscribers"),

        // (g['termination_reason'] == 'NO_ANSWER').sum()
        sum(when(col("termination_reason") === "NO_ANSWER", 1).otherwise(0)).as("total_no_answer"),

        // (g['termination_reason'] == 'USER_TERMINATED').sum()
        sum(when(col("termination_reason") === "USER_TERMINATED", 1).otherwise(0)).as("total_user_terminated"),        

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

    // #################################################
    // Superset table
    val voiceGlobal = voiceKpis.select(
    col("call_date").as("kpi_date"),
    lit("voice").as("service_type"),
    col("total_number_calls").as("total_transactions"),
    col("total_call_success").as("total_success"),
    col("total_call_failed").as("total_failed"),
    
    // Equivalent to: CAST(total_call_success AS DOUBLE) / NULLIF(total_number_calls, 0) * 100
    (col("total_call_success").cast(DoubleType) / 
      when(col("total_number_calls") === 0, lit(null))
      .otherwise(col("total_number_calls")) * 100).as("success_rate"),
      
    col("total_revenue"),
    col("total_call_duration").as("total_duration_seconds"),
    col("total_duration_of_minutes").as("total_duration_minutes"),
    col("average_call_duration").as("avg_duration_seconds"),
    
    // Casting NULLs to specific Data Types
    lit(null).cast(DoubleType).as("total_volume_gb"),
    lit(null).cast(LongType).as("total_bytes_uploaded"),
    lit(null).cast(LongType).as("total_bytes_downloaded"),
    lit(null).cast(DoubleType).as("avg_throughput"),
    
    col("unique_calling_subscribers").as("unique_subscribers"),
    col("total_no_answer"),
    
    lit(null).cast(LongType).as("total_network_error"),
    col("total_user_terminated"),
    lit(null).cast(LongType).as("total_quota_exceeded"),
    lit(null).cast(LongType).as("total_pending")
    )

    // Write  to Gold Table : voice_global
    logInfo(s"Writing Voice global KPIs for superset")
    voiceGlobal.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.voice_global")
    logInfo(s"Data successfully written to Iceberg table: gold.voice_global")

    // Superset table End
    // #################################################

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

        // count of unique caller_msisdn
        countDistinct("caller_msisdn").as("unique_calling_subscribers"),

        // count of unique callee_msisdn
        countDistinct("callee_msisdn").as("unique_receiving_subscribers"),

        // (g['termination_reason'] == 'NO_ANSWER').sum()
        sum(when(col("termination_reason") === "NO_ANSWER", 1).otherwise(0)).as("total_no_answer"),

        // (g['termination_reason'] == 'USER_TERMINATED').sum()
        sum(when(col("termination_reason") === "USER_TERMINATED", 1).otherwise(0)).as("total_user_terminated"),

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

    // Read Reference Tower Data
    val refTower = spark.read.table("referentiel.cell_ref")

    // #################################################
    // Superset table
    // Apply aliases to the DataFrames to mimic the SQL behavior
    val v = voiceTowerKpis.alias("v")
    val r = refTower.alias("r")

    // Perform the Left Join and Select the columns
    val VoiceJoinTower = v.join(r, Seq("cell_id"), "left")
      .select(
        col("v.call_date").as("kpi_date"),
        col("v.call_hour").as("kpi_hour"),
        col("cell_id"), // Automatically resolved from the Seq("cell_id") join condition
        lit("voice").as("service_type"),
        
        // Casting to LongType (Equivalent to BIGINT)
        col("v.total_number_calls").cast(LongType).as("total_transactions"),
        col("v.total_call_success").cast(LongType).as("total_success"),
        col("v.total_call_failed").cast(LongType).as("total_failed"),
        
        // Equivalent to: CAST(total_call_success AS DOUBLE) / NULLIF(CAST(total_number_calls AS DOUBLE), 0) * 100
        (col("v.total_call_success").cast(DoubleType) / 
          when(col("v.total_number_calls") === 0, lit(null))
          .otherwise(col("v.total_number_calls").cast(DoubleType)) * 100).as("success_rate"),
          
        // Casting remaining voice columns
        col("v.total_revenue").cast(DoubleType).as("total_revenue"),
        col("v.total_call_duration").cast(LongType).as("total_duration_seconds"),
        
        // Injecting NULLs with appropriate types
        lit(null).cast(DoubleType).as("total_volume_gb"),
        lit(null).cast(LongType).as("total_bytes_uploaded"),
        lit(null).cast(LongType).as("total_bytes_downloaded"),
        
        col("v.unique_calling_subscribers").cast(LongType).as("unique_subscribers"),
        col("v.total_no_answer").cast(LongType).as("total_no_answer"),
        
        lit(null).cast(LongType).as("total_network_error"),
        col("v.total_user_terminated").cast(LongType).as("total_user_terminated"),
        lit(null).cast(LongType).as("total_quota_exceeded"),
        lit(null).cast(LongType).as("total_pending"),
        
        // Selecting reference columns from 'r'
        col("r.region"),
        col("r.province"),
        col("r.latitude"),
        col("r.longitude"),
        col("r.technology"),
        col("r.capacity_erlang")
      )

    // Write  to Gold Table : voice_tower
    logInfo(s"Writing Voice global KPIs for superset")
    voiceJoinTower.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.voice_tower")
    logInfo(s"Data successfully written to Iceberg table: gold.voice_tower")

    // Superset table End
    // #################################################



    logInfo("Stopping spark session")
    spark.stop()
  }
}
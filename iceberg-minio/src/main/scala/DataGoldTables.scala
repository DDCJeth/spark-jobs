import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

        // count of unique msisdn
        countDistinct("msisdn").as("unique_subscribers"),

        // (g['session_end_reason'] == 'NETWORK_ERROR').sum()
        sum(when(col("session_end_reason") === "NETWORK_ERROR", 1).otherwise(0)).as("total_network_error"),

        // (g['session_end_reason'] == 'USER_TERMINATED').sum()
        sum(when(col("session_end_reason") === "USER_TERMINATED", 1).otherwise(0)).as("total_user_terminated"),

        // (g['session_end_reason'] == 'QUOTA_EXCEEDED').sum()
        sum(when(col("session_end_reason") === "QUOTA_EXCEEDED", 1).otherwise(0)).as("total_quota_exceeded"),

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


    // #################################################
    // Superset table
    val dataGlobal = dataKpis.select(
      col("session_date").as("kpi_date"),
      lit("data").as("service_type"),
      col("total_sessions").as("total_transactions"),
      col("total_active_sessions").as("total_success"),
      
      // Arithmetic: CAST(total_sessions - total_active_sessions AS BIGINT)
      (col("total_sessions") - col("total_active_sessions")).cast(LongType).as("total_failed"),
      
      // Equivalent to: CAST(total_active_sessions AS DOUBLE) / NULLIF(total_sessions, 0) * 100
      (col("total_active_sessions").cast(DoubleType) / 
        when(col("total_sessions") === 0, lit(null))
        .otherwise(col("total_sessions")) * 100).as("success_rate"),
        
      col("total_revenue"),
      
      // Casting NULLs to specific Data Types
      lit(null).cast(LongType).as("total_duration_seconds"),
      
      col("average_session_duration_minutes").as("total_duration_minutes"),
      col("average_session_duration_sec").as("avg_duration_seconds"),
      col("total_data_volume_GB").as("total_volume_gb"),
      col("total_bytes_uploaded"),
      col("total_bytes_downloaded"),
      col("average_throughput_per_session").as("avg_throughput"),
      col("unique_subscribers"),
      
      lit(null).cast(LongType).as("total_no_answer"),
      col("total_network_error"),
      col("total_user_terminated"),
      col("total_quota_exceeded"),
      lit(null).cast(LongType).as("total_pending")
    )

    // Write  to Gold Table : data_global
    logInfo(s"Writing Data global KPIs for superset")
    dataGlobal.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.data_global")
    logInfo(s"Data successfully written to Iceberg table: gold.data_global")

    // Superset table End
    // #################################################




    val dataTowerKpis = df.groupBy("session_date", "session_hour", "cell_id")
      .agg(
        count("session_id").as("total_sessions"),
        count(when(col("session_end_reason") === "NORMAL", 1)).as("total_active_sessions"),
        sum("bytes_uploaded").as("total_bytes_uploaded"),
        sum("bytes_downloaded").as("total_bytes_downloaded"),
        sum("charging_amount").as("total_revenue"),
        // count of unique caller_msisdn
        countDistinct("msisdn").as("unique_subscribers"),

        // (g['session_end_reason'] == 'NETWORK_ERROR').sum()
        sum(when(col("session_end_reason") === "NETWORK_ERROR", 1).otherwise(0)).as("total_network_error"),

        // (g['session_end_reason'] == 'USER_TERMINATED').sum()
        sum(when(col("session_end_reason") === "USER_TERMINATED", 1).otherwise(0)).as("total_user_terminated"),

        // (g['session_end_reason'] == 'QUOTA_EXCEEDED').sum()
        sum(when(col("session_end_reason") === "QUOTA_EXCEEDED", 1).otherwise(0)).as("total_quota_exceeded")
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

    // #################################################
    // Superset table
    // Apply aliases to the DataFrames to mimic the SQL behavior
    // Read Reference Tower Data
    val refTower = spark.read.table("referentiel.cell_ref")


    // Apply aliases to the DataFrames to mimic the SQL behavior
    val d = dataTowerKpis.alias("d")
    val r = refTower.alias("r")

    // Perform the Left Join and Select the columns
    val dataJoinTower = d.join(r, Seq("cell_id"), "left")
      .select(
        col("d.session_date").as("kpi_date"),
        col("d.session_hour").as("kpi_hour"),
        col("cell_id"), // Automatically resolved from the Seq("cell_id") join condition
        lit("data").as("service_type"),
        
        // Casting explicit metrics to LongType (BIGINT)
        col("d.total_sessions").cast(LongType).as("total_transactions"),
        col("d.total_active_sessions").cast(LongType).as("total_success"),
        
        // Arithmetic: CAST(total_sessions AS BIGINT) - CAST(total_active_sessions AS BIGINT)
        (col("d.total_sessions").cast(LongType) - col("d.total_active_sessions").cast(LongType)).as("total_failed"),
        
        // Equivalent to: CAST(total_active_sessions AS DOUBLE) / NULLIF(CAST(total_sessions AS DOUBLE), 0) * 100
        (col("d.total_active_sessions").cast(DoubleType) / 
          when(col("d.total_sessions") === 0, lit(null))
          .otherwise(col("d.total_sessions").cast(DoubleType)) * 100).as("success_rate"),
          
        // Casting remaining data columns
        col("d.total_revenue").cast(DoubleType).as("total_revenue"),
        lit(null).cast(LongType).as("total_duration_seconds"), // Placeholder for data
        
        col("d.total_data_volume_gb").cast(DoubleType).as("total_volume_gb"),
        col("d.total_bytes_uploaded").cast(LongType).as("total_bytes_uploaded"),
        col("d.total_bytes_downloaded").cast(LongType).as("total_bytes_downloaded"),
        col("d.unique_subscribers").cast(LongType).as("unique_subscribers"),
        
        // Injecting NULLs with appropriate types for voice-specific metrics
        lit(null).cast(LongType).as("total_no_answer"),
        
        col("d.total_network_error").cast(LongType).as("total_network_error"),
        col("d.total_user_terminated").cast(LongType).as("total_user_terminated"),
        col("d.total_quota_exceeded").cast(LongType).as("total_quota_exceeded"),
        lit(null).cast(LongType).as("total_pending"),
        
        // Selecting reference columns from 'r'
        col("r.region"),
        col("r.province"),
        col("r.latitude"),
        col("r.longitude"),
        col("r.technology"),
        col("r.capacity_erlang")
      )

    // Write  to Gold Table : data_tower
    logInfo(s"Writing Voice global KPIs for superset")
    dataJoinTower.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.data_tower")
    logInfo(s"Data successfully written to Iceberg table: gold.data_tower")

    // Superset table End
    // #################################################


    logInfo("Stopping spark session")
    spark.stop()
  }
}
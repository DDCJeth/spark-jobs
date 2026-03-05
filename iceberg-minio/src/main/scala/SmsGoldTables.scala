import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
      countDistinct("sender_msisdn").as("unique_sending_subscribers"),
      countDistinct("receiver_msisdn").as("unique_receiving_subscribers"),

      // (g['delivery_status'] == 'PENDING').sum()
      sum(when(col("delivery_status") === "PENDING", 1).otherwise(0)).as("total_pending_sms")

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


    // #################################################
    // Superset table
    // Apply aliases to the DataFrames to mimic the SQL behavior
    // Assuming your source DataFrame is named 'smsKpis'

    val smsGlobal = smsKpis.select(
      col("sms_date").as("kpi_date"),
      lit("sms").as("service_type"),
      col("total_sms").as("total_transactions"),
      col("total_delivered_sms").as("total_success"),
      col("total_failed_sms").as("total_failed"),
      
      // Equivalent to: CAST(total_delivered_sms AS DOUBLE) / NULLIF(total_sms, 0) * 100
      (col("total_delivered_sms").cast(DoubleType) / 
        when(col("total_sms") === 0, lit(null))
        .otherwise(col("total_sms")) * 100).as("success_rate"),
        
      col("total_revenue"),
      
      // Casting NULLs to specific Data Types to align schemas
      lit(null).cast(LongType).as("total_duration_seconds"),
      lit(null).cast(DoubleType).as("total_duration_minutes"),
      lit(null).cast(DoubleType).as("avg_duration_seconds"),
      lit(null).cast(DoubleType).as("total_volume_gb"),
      lit(null).cast(LongType).as("total_bytes_uploaded"),
      lit(null).cast(LongType).as("total_bytes_downloaded"),
      lit(null).cast(DoubleType).as("avg_throughput"),
      
      col("unique_sending_subscribers").as("unique_subscribers"),
      
      lit(null).cast(LongType).as("total_no_answer"),
      lit(null).cast(LongType).as("total_network_error"),
      lit(null).cast(LongType).as("total_user_terminated"),
      lit(null).cast(LongType).as("total_quota_exceeded"),
      
      col("total_pending_sms").as("total_pending")
    )


    // Write  to Gold Table : sms_global
    logInfo(s"Writing SMS global KPIs for superset")
    smsGlobal.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.sms_global")
    logInfo(s"Data successfully written to Iceberg table: gold.sms_global")

    // Superset table End
    // #################################################
  

    val smsTowerKpis = df.groupBy("sms_date", "sms_hour", "cell_id")
      .agg(
        count("sms_id").as("total_sms"),
        count(when(col("delivery_status") === "DELIVERED", 1)).as("total_delivered_sms"),
        count(when(col("delivery_status") === "FAILED", 1)).as("total_failed_sms"),
        sum("charging_amount").as("total_revenue"),
        
        countDistinct("sender_msisdn").as("unique_sending_subscribers"),
        countDistinct("receiver_msisdn").as("unique_receiving_subscribers"),
        // (g['delivery_status'] == 'PENDING').sum()
        sum(when(col("delivery_status") === "PENDING", 1).otherwise(0)).as("total_pending_sms")

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

    // Read Reference Tower Data
    val refTower = spark.read.table("referentiel.cell_ref")

    // #################################################
    // Superset table
    // Apply aliases to the DataFrames to mimic the SQL behavior
    val s = smsTowerKpis.alias("s")
    val r = refTower.alias("r")

    // Perform the Left Join and Select the columns
    val smsJoinTower = s.join(r, Seq("cell_id"), "left")
      .select(
        col("s.sms_date").as("kpi_date"),
        col("s.sms_hour").as("kpi_hour"),
        col("cell_id"), // Automatically resolved from the Seq("cell_id") join condition
        lit("sms").as("service_type"),
        
        // Casting explicit metrics to LongType (BIGINT)
        col("s.total_sms").cast(LongType).as("total_transactions"),
        col("s.total_delivered_sms").cast(LongType).as("total_success"),
        col("s.total_failed_sms").cast(LongType).as("total_failed"),
        
        // Equivalent to: CAST(total_delivered_sms AS DOUBLE) / NULLIF(CAST(total_sms AS DOUBLE), 0) * 100
        (col("s.total_delivered_sms").cast(DoubleType) / 
          when(col("s.total_sms") === 0, lit(null))
          .otherwise(col("s.total_sms").cast(DoubleType)) * 100).as("success_rate"),
          
        col("s.total_revenue").cast(DoubleType).as("total_revenue"),
        
        // Injecting NULLs with appropriate types for voice/data specific metrics
        lit(null).cast(LongType).as("total_duration_seconds"),
        lit(null).cast(DoubleType).as("total_volume_gb"),
        lit(null).cast(LongType).as("total_bytes_uploaded"),
        lit(null).cast(LongType).as("total_bytes_downloaded"),
        
        col("s.unique_sending_subscribers").cast(LongType).as("unique_subscribers"),
        
        lit(null).cast(LongType).as("total_no_answer"),
        lit(null).cast(LongType).as("total_network_error"),
        lit(null).cast(LongType).as("total_user_terminated"),
        lit(null).cast(LongType).as("total_quota_exceeded"),
        
        col("s.total_pending_sms").cast(LongType).as("total_pending"),
        
        // Selecting reference columns from 'r'
        col("r.region"),
        col("r.province"),
        col("r.latitude"),
        col("r.longitude"),
        col("r.technology"),
        col("r.capacity_erlang")
      )

    // Write  to Gold Table : sms_tower
    logInfo(s"Writing Sms global KPIs for superset")
    smsJoinTower.write
      .mode(SaveMode.Append)
      .saveAsTable("gold.sms_tower")
    logInfo(s"Data successfully written to Iceberg table: gold.sms_tower")

    // Superset table End
    // #################################################



    logInfo("Stopping spark session")
    spark.stop()
  }
}
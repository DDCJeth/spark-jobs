import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode

object DataGoldStream {
  def main(args: Array[String]): Unit = {

    // 1. Initialize Spark
    val spark = SparkSession.builder()
      .appName("KafkaVoiceMultiKPIs")
      .getOrCreate()

    import spark.implicits._

    // 2. Define Schema
    // 1. Updated Schema based on your sample JSON
    val dataSchema = new StructType()
        .add("timestamp", StringType)         // "2024-12-16T11:21:08"
        .add("session_id", StringType)
        .add("session_date", StringType)
        .add("session_hour", IntegerType)
        .add("msisdn", LongType)       // Handled as Long for numbers
        .add("apn", StringType)
        .add("session_duration_seconds", LongType)
        .add("bytes_uploaded", LongType)
        .add("bytes_downloaded", LongType)
        .add("cell_id", StringType)
        .add("region", StringType)
        .add("session_end_reason", StringType)
        .add("charging_amount", DoubleType)
        .add("filename", StringType)
        .add("ingest_ts", StringType)

    // 3. Read Source (Shared Input)
    val rawKafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data-silver-cdr") // Input topic for Silver layer
      .option("startingOffsets", "latest")
      .load()

    val parsedDf = rawKafkaDf
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json($"json_string", dataSchema).as("data"))
      .select("data.*")

    // ==========================================
    // TRANSFORMATION 1: Daily Voice KPIs
    // ==========================================

  // Assuming your dataframe is named 'dataDf'
  val dataKpis = parsedDf.groupBy("session_date")
    .agg(
      // 1. Basic Counts
      count("session_id").as("total_sessions"),

      // 2. Conditional Count (Active Sessions)
      // We count '1' only when the condition is met.
      count(when(col("session_end_reason") === "NORMAL", 1)).as("total_active_sessions"),

      // 3. Sums (using coalesce to treat nulls as 0 to avoid breaking the sum)
      sum(coalesce(col("bytes_uploaded"), lit(0))).as("total_bytes_uploaded"),
      sum(coalesce(col("bytes_downloaded"), lit(0))).as("total_bytes_downloaded"),

      // 4. Calculated Sums
      sum(
        coalesce(col("bytes_uploaded"), lit(0)) + 
        coalesce(col("bytes_downloaded"), lit(0))
      ).as("total_bytes"),

      sum("charging_amount").as("total_revenue"),
      
    )
    .withColumn("total_data_volume_GB", col("total_bytes") / math.pow(1024, 3))

    // Prepare Output 1
    val queryDaily = dataKpis
      .select(
        $"session_date".as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("DailyKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "data-gold-cdr") // Topic 1
      .option("checkpointLocation", "/tmp/checkpoints/data-daily-kpis")
      .outputMode(OutputMode.Update())
      .start()

    // ==========================================
    // TRANSFORMATION 2: Tower/Hourly KPIs
    // ==========================================
    val dataTowerKpis = parsedDf.groupBy($"session_date", $"session_hour", $"cell_id")
    .agg(
      // 1. Basic Counts
      count("session_id").as("total_sessions"),

      // 2. Conditional Count (Active Sessions)
      // We count '1' only when the condition is met.
      count(when(col("session_end_reason") === "NORMAL", 1)).as("total_active_sessions"),

      // 3. Sums (using coalesce to treat nulls as 0 to avoid breaking the sum)
      sum(coalesce(col("bytes_uploaded"), lit(0))).as("total_bytes_uploaded"),
      sum(coalesce(col("bytes_downloaded"), lit(0))).as("total_bytes_downloaded"),

      // 4. Calculated Sums
      sum(
        coalesce(col("bytes_uploaded"), lit(0)) + 
        coalesce(col("bytes_downloaded"), lit(0))
      ).as("total_bytes"),

      sum("charging_amount").as("total_revenue"),
      
    )
    .withColumn("total_data_volume_GB", col("total_bytes") / math.pow(1024, 3))


    // Prepare Output 2
    // For the Kafka key, we concatenate columns to make a unique ID (e.g. "2024-12-14_11_CELL_01")
    val queryTower = dataTowerKpis
      .select(
        concat_ws("_", $"session_date", $"session_hour", $"cell_id").as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("TowerKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "data-gold-tower-cdr") // Topic 2
      .option("checkpointLocation", "/tmp/checkpoints/data-tower-kpis")
      .outputMode(OutputMode.Update())
      .start()

    // 4. Wait for All Streams
    spark.streams.awaitAnyTermination()
  }
}
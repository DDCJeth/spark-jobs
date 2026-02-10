import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode

object SmsGoldStream {
  def main(args: Array[String]): Unit = {

    // 1. Initialize Spark
    val spark = SparkSession.builder()
      .appName("KafkaSmsMultiKPIs")
      .getOrCreate()

    import spark.implicits._

    // 2. Define Schema
    val cdrSchema = new StructType()
          .add("timestamp", StringType)
          .add("sms_id", StringType)
          .add("sms_date", StringType)
          .add("sms_hour", IntegerType)
          .add("sender_msisdn", LongType)
          .add("receiver_msisdn", LongType)
          .add("sms_type", StringType)
          .add("message_length", IntegerType)
          .add("cell_id", StringType)
          .add("region", StringType)
          .add("delivery_status", StringType)
          .add("charging_amount", DoubleType)
          .add("filename", StringType)
          .add("ingest_ts", StringType)

    // 3. Read Source (Shared Input)
    val rawKafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sms-silver-cdr") // sms input topic for Silver layer
      .option("startingOffsets", "latest")
      .load()

    val parsedDf = rawKafkaDf
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json($"json_string", cdrSchema).as("data"))
      .select("data.*")

    // ==========================================
    // TRANSFORMATION 1: Daily sms KPIs
    // ==========================================
    val smsKpis = parsedDf.groupBy($"sms_date")
      .agg(
        count($"sms_id").as("total_number_sms"),
        sum(when($"delivery_status" === "DELIVERED", 1).otherwise(0)).as("total_delivered_sms"),
        sum(when($"delivery_status" === "FAILED", 1).otherwise(0)).as("total_failed_sms"),
        sum($"charging_amount").as("total_revenue"),
        approx_count_distinct($"sender_msisdn").as("total_subscribers_sending_sms"),
        approx_count_distinct($"receiver_msisdn").as("total_subscribers_receiving_sms")
      )

    // Prepare Output 1
    val queryDaily = smsKpis
      .select(
        $"sms_date".as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("DailyKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sms-gold-cdr") // Topic 1
      .option("checkpointLocation", "/tmp/checkpoints/sms-daily-kpis")
      .outputMode(OutputMode.Update())
      .start()

    // ==========================================
    // TRANSFORMATION 2: Tower/Hourly KPIs
    // ==========================================
    val smsTowerKpis = parsedDf.groupBy($"sms_date", $"sms_hour", $"cell_id")
      .agg(
        count($"sms_id").as("total_number_sms"),
        sum(when($"delivery_status" === "DELIVERED", 1).otherwise(0)).as("total_delivered_sms"),
        sum(when($"delivery_status" === "FAILED", 1).otherwise(0)).as("total_failed_sms"),
        sum($"charging_amount").as("total_revenue"),
        approx_count_distinct($"sender_msisdn").as("total_subscribers_sending_sms"),
        approx_count_distinct($"receiver_msisdn").as("total_subscribers_receiving_sms")
      )

    // Prepare Output 2
    // For the Kafka key, we concatenate columns to make a unique ID (e.g. "2024-12-14_11_CELL_01")
    val queryTower = smsTowerKpis
      .select(
        concat_ws("_", $"sms_date", $"sms_hour", $"cell_id").as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("TowerKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sms-gold-tower-cdr") // Topic 2
      .option("checkpointLocation", "/tmp/checkpoints/sms-tower-kpis")
      .outputMode(OutputMode.Update())
      .start()

    // 4. Wait for All Streams
    spark.streams.awaitAnyTermination()
  }
}
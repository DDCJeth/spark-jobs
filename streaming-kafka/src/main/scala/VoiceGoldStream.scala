import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode

object VoiceGoldStream {
  def main(args: Array[String]): Unit = {

    // Expected args: <kafkaHost> <bronzeTopic> <goldTopic> <checkpointLocation> 
    // Expected args: 1. kafkaHost (e.g. localhost:9092)
    //                2. silverTopic (e.g. voice-bronze-cdr) 
    //                3. goldTopic (e.g. voice-gold-cdr) )
    //                4. checkpointLocation (e.g. /tmp/checkpoints/voice-gold-cdr or s3a://datalake/checkpoints/voice-gold-cdr)

    if (args.length < 4) {
      println("Usage: PopulateGoldTables <kafkaHost> <silverTopic> <goldTopic> <checkpointLocation>")    
      sys.exit(1)
    }

    val kafkaHost      = args(0) // e.g. localhost:9092
    val silverTopic    = args(1) // e.g. voice-bronze-cdr
    val goldTopic      = args(2) // e.g. voice-gold-cdr
    val checkpointLocation  = args(3) // e.g. /tmp/checkpoints/voice-gold-cdr or s3a://datalake/checkpoints/voice-gold-cdr


    // 1. Initialize Spark
    val spark = SparkSession.builder()
      .appName("KafkaVoiceMultiKPIs")
      .getOrCreate()

    import spark.implicits._

    // 2. Define Schema
    val cdrSchema = new StructType()
          .add("timestamp", StringType)
          .add("call_id", StringType)
          .add("call_date", StringType)
          .add("call_hour", IntegerType)
          .add("caller_msisdn", LongType)
          .add("callee_msisdn", LongType)
          .add("call_type", StringType)
          .add("duration_minutes", DoubleType)
          .add("duration_seconds", IntegerType)
          .add("cell_id", StringType)
          .add("region", StringType)
          .add("termination_reason", StringType)
          .add("call_status", StringType)
          .add("charging_amount", DoubleType)
          .add("filename", StringType)
          .add("ingest_ts", StringType)

    // 3. Read Source (Shared Input)
    val rawKafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost) // e.g. localhost:9092
      .option("subscribe", silverTopic) // Input topic for Silver layer
      .option("startingOffsets", "latest")
      .load()

    val parsedDf = rawKafkaDf
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json($"json_string", cdrSchema).as("data"))
      .select("data.*")

    // ==========================================
    // TRANSFORMATION 1: Daily Voice KPIs
    // ==========================================
    val voiceKpis = parsedDf.groupBy($"call_date")
      .agg(
        count($"call_id").as("total_number_calls"),
        sum($"duration_seconds").as("total_call_duration"),
        avg($"duration_seconds").as("average_call_duration"),
        avg(when($"call_status" === "SUCCESS", $"duration_seconds")).as("average_call_duration_success"),
        sum(when($"call_status" === "SUCCESS", 1).otherwise(0)).as("total_call_success"),
        sum(when($"call_status" === "FAILED", 1).otherwise(0)).as("total_call_failed"),
        sum($"charging_amount").as("total_revenue"),
        sum($"duration_minutes").as("total_duration_of_minutes")
      )

    // Prepare Output 1
    val queryDaily = voiceKpis
      .select(
        $"call_date".as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("DailyKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost) // Replace with your Kafka Broker
      .option("topic", goldTopic) // Topic 1
      .option("checkpointLocation", checkpointLocation) // Required for fault tolerance
      .outputMode(OutputMode.Update())
      .start()

    // ==========================================
    // TRANSFORMATION 2: Tower/Hourly KPIs
    // ==========================================
    val voiceTowerKpis = parsedDf.groupBy($"call_date", $"call_hour", $"cell_id")
      .agg(
        count($"call_id").as("total_number_calls"),
        sum($"duration_seconds").as("total_call_duration"),
        sum(when($"call_status" === "SUCCESS", 1).otherwise(0)).as("total_call_success"),
        sum(when($"call_status" === "FAILED", 1).otherwise(0)).as("total_call_failed"),
        sum($"charging_amount").as("total_revenue")
      )

    // Prepare Output 2
    // For the Kafka key, we concatenate columns to make a unique ID (e.g. "2024-12-14_11_CELL_01")
    val queryTower = voiceTowerKpis
      .select(
        concat_ws("_", $"call_date", $"call_hour", $"cell_id").as("key"),
        to_json(struct("*")).as("value")
      )
      .writeStream
      .queryName("TowerKPIs")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost) // Replace with your Kafka Broker
      .option("topic", s"$goldTopic-tower") // Topic 2
      .option("checkpointLocation", s"$checkpointLocation-tower")
      .outputMode(OutputMode.Update())
      .start()

    // 4. Wait for All Streams
    spark.streams.awaitAnyTermination()
  }
}
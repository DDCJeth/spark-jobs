import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VoiceSilverStream {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaVoiceTransformation")
      .getOrCreate()

    import spark.implicits._

    // 1. Updated Schema based on your sample JSON
    val voiceSchema = new StructType()
      .add("timestamp", StringType)         // "2024-12-16T11:21:08"
      .add("call_id", StringType)
      .add("caller_msisdn", LongType)       // Handled as Long for numbers
      .add("callee_msisdn", LongType)
      .add("call_type", StringType)
      .add("duration_seconds", LongType)
      .add("cell_id", StringType)
      .add("region", StringType)
      .add("termination_reason", StringType)
      .add("charging_amount", DoubleType)
      .add("filename", StringType)
      .add("ingest_ts", StringType)

    // 2. Read from Kafka
    val kafkaRawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "voice-bronze-cdr") // Input topic for Voice Bronze layer
      .load()

    // 3. Parse JSON and Flatten
    val voiceDf = kafkaRawDf
      .select(from_json($"value".cast("string"), voiceSchema).as("data"))
      .select("data.*")

    // 4. Apply Transformations
    val processedDf = voiceDf
      // Parse ISO 8601 Timestamp (handling the 'T')
      .withColumn("ts_parsed", to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
      
      // Derive Date and Hour
      .withColumn("call_date", to_date($"ts_parsed"))
      .withColumn("call_hour", hour($"ts_parsed"))
      
      // Compute Duration in Minutes
      .withColumn("duration_minutes", $"duration_seconds" / 60.0)
      
      // Derive Call Status
      .withColumn("call_status", 
        when($"termination_reason".isInCollection(Seq("NORMAL", "USER_TERMINATED")), "SUCCESS")
        .otherwise("FAILED")
      )

    // 5. Final Selection to match your requested Indices (2, 4, 7, etc.)
    // Logic: 
    // Index 0: timestamp
    // Index 1: call_id
    // Index 2: call_date (Inserted)
    // Index 3: call_hour (Inserted)
    // Index 4: caller_msisdn (Original Index 2 shifted)
    // ... and so on
    val finalDf = processedDf.select(
      $"timestamp",           // 0
      $"call_id",             // 1
      $"call_date",           // 2 (New)
      $"call_hour",           // 3 (New)
      $"caller_msisdn",       // 4
      $"callee_msisdn",       // 5
      $"call_type",           // 6
      $"duration_minutes",    // 7 (New)
      $"duration_seconds",    // 8
      $"cell_id",             // 9
      $"region",
      $"termination_reason",
      $"call_status",         // Derived
      $"charging_amount",
      $"filename",
      $"ingest_ts"
    )


    // 6. Write output into kafka topic named silver-voice-data
    val query = finalDf
      // Kafka requires a 'value' column. We serialize the entire row to JSON.
      // If you have a primary key, cast it to string as the 'key' (e.g., "CAST(id as STRING) as key")
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Replace with your Kafka Broker
      .option("topic", "voice-silver-cdr") // Output topic
      .option("checkpointLocation", "/tmp/checkpoints/voice-silver-cdr") // Required for fault tolerance
      .outputMode("append")
      .start()

    // 6. Write output
    // val query = finalDf.writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("truncate", "false")
    //   .start()

    query.awaitTermination()
  }
}
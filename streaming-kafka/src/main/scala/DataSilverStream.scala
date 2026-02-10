import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataSilverStream {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaDataTransformation")
      .getOrCreate()

    import spark.implicits._


    // 1. Updated Schema based on your sample JSON
    val dataSchema = new StructType()
      .add("timestamp", StringType)         // "2024-12-16T11:21:08"
      .add("session_id", StringType)
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

    // 2. Read from Kafka
    val kafkaRawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data-bronze-cdr") // Input topic for Bronze layer
      .option("startingOffsets", "latest")
      .load()

    // 3. Parse JSON and Flatten
    val dataDf = kafkaRawDf
      .select(from_json($"value".cast("string"), dataSchema).as("data"))
      .select("data.*")

    // 4. Apply Transformations
    val processedDf = dataDf
      // Parse ISO 8601 Timestamp (handling the 'T')
      .withColumn("ts_parsed", to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
      
      // Derive Date and Hour
      .withColumn("session_date", to_date($"ts_parsed"))
      .withColumn("session_hour", hour($"ts_parsed"))
      

    val finalDf = processedDf.select(
      $"timestamp",           // 0
      $"session_id",             // 1
      $"session_date",           // 2 (New)
      $"session_hour",           // 3 (New)
      $"msisdn",       // 4
      $"apn",           // 5
      $"session_duration_seconds",    // 6 (New)
      $"bytes_uploaded",    // 7
      $"bytes_downloaded",    // 8
      $"cell_id",             // 9
      $"region",
      $"session_end_reason",
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
      .option("topic", "data-silver-cdr") // Output topic
      .option("checkpointLocation", "/tmp/checkpoints/silver-data-cdr") // Required for fault tolerance
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
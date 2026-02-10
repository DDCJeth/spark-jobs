import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SmsSilverStream {
  def main(args: Array[String]): Unit = {

    // Expected args: <kafkaHost> <bronzeTopic> <silverTopic> <checkpointLocation> 
    // Expected args: 1. kafkaHost (e.g. localhost:9092)
    //                2. bronzeTopic (e.g. sms-bronze-cdr) 
    //                3. silverTopic (e.g. sms-silver-cdr) )
    //                4. checkpointLocation (e.g. /tmp/checkpoints/sms-silver-cdr or s3a://datalake/checkpoints/sms-silver-cdr)

    if (args.length < 4) {
      println("Usage: PopulateSilverTables <kafkaHost> <bronzeTopic> <silverTopic> <checkpointLocation>")    
      sys.exit(1)
    }

    val kafkaHost      = args(0) // e.g. localhost:9092
    val bronzeTopic    = args(1) // e.g. sms-bronze-cdr
    val silverTopic      = args(2) // e.g. sms-silver-cdr
    val checkpointLocation  = args(3) // e.g. /tmp/checkpoints/sms-silver-cdr or s3a://datalake/checkpoints/sms-silver-cdr

    val spark = SparkSession.builder()
      .appName("KafkaSmsTransformation")
      .getOrCreate()

    import spark.implicits._


    // 1. Updated Schema for SMS Data
    val smsSchema = new StructType()
      .add("timestamp", StringType)         // "2024-12-16T11:21:08"
      .add("sms_id", StringType)
      .add("sender_msisdn", LongType)       // Handled as Long for numbers
      .add("receiver_msisdn", LongType)
      .add("sms_type", StringType)
      .add("message_length", IntegerType)
      .add("cell_id", StringType)
      .add("region", StringType)
      .add("delivery_status", StringType)
      .add("charging_amount", DoubleType)
      .add("filename", StringType)
      .add("ingest_ts", StringType)

    // 2. Read from Kafka
    val kafkaRawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", bronzeTopic) // Input topic for SMS Bronze layer
      .option("startingOffsets", "latest")
      .load()

    // 3. Parse JSON and Flatten
    val smsDf = kafkaRawDf
      .select(from_json($"value".cast("string"), smsSchema).as("data"))
      .select("data.*")

    // 4. Apply Transformations
    val processedDf = smsDf
      // Parse ISO 8601 Timestamp (handling the 'T')
      .withColumn("ts_parsed", to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
      
      // Derive Date and Hour
      .withColumn("sms_date", to_date($"ts_parsed"))
      .withColumn("sms_hour", hour($"ts_parsed"))
      

    val finalDf = processedDf.select(
      $"timestamp",           // 0
      $"sms_id",              // 1
      $"sms_date",            // 2 (New)
      $"sms_hour",            // 3 (New)
      $"sender_msisdn",       // 4
      $"receiver_msisdn",       // 5
      $"sms_type",           // 6
      $"message_length",    // 7
      $"cell_id",             // 8
      $"region",
      $"delivery_status",
      $"charging_amount",
      $"filename",
      $"ingest_ts"
    )


    // 6. Write output into kafka topic named silver-sms-data
    val query = finalDf
      // Kafka requires a 'value' column. We serialize the entire row to JSON.
      // If you have a primary key, cast it to string as the 'key' (e.g., "CAST(id as STRING) as key")
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost) // Replace with your Kafka Broker
      .option("topic", silverTopic) // Output topic
      .option("checkpointLocation", checkpointLocation) // Required for fault tolerance
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
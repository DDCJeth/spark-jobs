import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import scala.io.Source

object OpenMetadataTest extends Logging {

  def main(args: Array[String]): Unit = {


    logInfo(s"Starting OpenMetadataTest job")

    val spark = SparkSession.builder()
      .appName(s"Lineage between Spark, OpenMetadata and Iceberg")
      .getOrCreate()

    logInfo("SparkSession created successfully")

    // 0. Create Namespace if not exists
    val namespace = "bronze"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
    logInfo(s"Namespace '$namespace' ensured")

    // 1. Create DataFrame with sample data
    logInfo(s"Create DataFrame with sample data")
    import spark.implicits._

    val sampleData = Seq(
      ("2024-01-15 10:00:00", "session1", "1234567890", "apn1", 300, 500000, 1000000, "normal", "cell1", "region1", 5.0),
      ("2024-01-15 11:00:00", "session2", "0987654321", "apn2", 600, 1000000, 2000000, "normal", "cell2", "region2", 10.0)
    ).toDF("timestamp", "session_id", "msisdn", "apn", "session_duration_seconds", "bytes_uploaded", "bytes_downloaded", "session_end_reason", "cell_id", "region", "charging_amount")


    sampleData.show(5)

    // 2. Transformation
    val enrichedDf = sampleData.withColumn("ingestion_date", current_date())
    logInfo(s"Added ingestion_date column. Final row count: ${enrichedDf.count()}")
    
    // 3. Write to Bronze Iceberg Table
    val targetTable = "bronze.fake_data"
    enrichedDf.write
      .mode(SaveMode.Append)
      .saveAsTable(targetTable)
    logInfo(s"Successfully wrote data")

    spark.stop()
    logInfo("SparkSession stopped. Job completed successfully")
  }
}
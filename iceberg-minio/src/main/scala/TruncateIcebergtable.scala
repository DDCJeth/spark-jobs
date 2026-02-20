import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging

object TruncateIcebergTable extends Logging {

  def main(args: Array[String]): Unit = {
    // Expected args: 1. schema (e.g. bronze ; silver ; gold) 
    // Expected args: 2. log_type (e.g. voice ; data ; sms)
    if (args.length < 2) {
      logError("Usage: TruncateIcebergTable <schema> <log_type>")
      sys.exit(1)
    }

    val schema    = args(0)
    val logType   = args(1)
    val tableName = s"$schema.$logType"

    val spark = SparkSession.builder()
      .appName(s"Truncate Iceberg Table: $tableName")
      .getOrCreate()

    try {
      logInfo(s"Starting truncation process for Iceberg table: $tableName")

      // Execute the TRUNCATE TABLE statement
      // This removes all data records but preserves the table schema and properties.
      spark.sql(s"TRUNCATE TABLE $tableName")

      logInfo(s"Successfully truncated table $tableName. Data is cleared, schema is intact.")

    } catch {
      case e: Exception =>
        logError(s"Failed to truncate table $tableName", e)
        sys.exit(1)
    } finally {
      logInfo("Stopping Spark session")
      spark.stop()
    }
  }
}
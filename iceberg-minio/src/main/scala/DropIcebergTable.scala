import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging

object DropIcebergTable extends Logging {

  def main(args: Array[String]): Unit = {
    // Expected args: 1. schema (e.g. bronze ; silver ; gold)
    // Expected args: 1. log_type (e.g. voice ; data ; sms)
    if (args.length < 2) {
      logError("Usage: DropIcebergTable <schema> <log_type>")
      sys.exit(1)
    }

    val schema    = args(0)
    val logType   = args(1)
    val tableName = s"$schema.$logType"

    val spark = SparkSession.builder()
      .appName(s"Drop Iceberg Table: $tableName")
      .getOrCreate()

    try {
      logInfo(s"Starting deletion process for Iceberg table: $tableName")

      // Execute the DROP TABLE statement
      // Note: Depending on your Iceberg catalog (e.g., AWS Glue, Nessie, Hive),
      // dropping the table generally removes both metadata and data files.
      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      logInfo(s"Successfully dropped table $tableName (if it existed).")

    } catch {
      case e: Exception =>
        logError(s"Failed to drop table $tableName", e)
        sys.exit(1)
    } finally {
      logInfo("Stopping Spark session")
      spark.stop()
    }
  }
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object CreateIcebergTable extends Logging {

  def main(args: Array[String]): Unit = {
    // Expected args: 1. s3_base_path (e.g. s3://datalake/schemas/) 
    //                2. log_type (e.g. voice ; data ; sms )
    if (args.length < 2) {
      logError("Usage: CreateIcebergTable <schema_base_path> <log_type>")
      sys.exit(1)
    }

    val schemaBasePath = args(0).stripSuffix("/")
    val logType        = args(1)
    val schemaPath     = s"$schemaBasePath/${logType}_schema.json"
    val tableName      = s"bronze.$logType"

    val spark = SparkSession.builder()
      .appName(s"Create Iceberg Table: $tableName")
      .getOrCreate()

    try {
      logInfo(s"Loading schema from: $schemaPath")
      
      // 1. Read the JSON schema string from S3
      val schemaJson = spark.sparkContext.textFile(schemaPath).collect().mkString
      
      val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]

      // 2. Add the ingestion_date column (since it's not in your raw schema files)
      val finalSchema = schema.add("ingestion_date", "date")

      logInfo(s"Schema Loaded: $finalSchema")

      logInfo(s"Creating Namespace 'bronze' if not exists")
      // 3. Create Namespace
      spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

      logInfo(s" Namespace created or already exists.")

      // 4. Build and Execute Table Creation
      logInfo(s"Creating Iceberg table: $tableName")
      
      // 1. Convert the StructType fields into a comma-separated SQL string
      // Example: "timestamp TIMESTAMP, sms_id STRING, ingestion_date DATE"
      val ddlSchema = finalSchema.fields.map { field =>
        s"`${field.name}` ${field.dataType.sql}"
      }.mkString(", ")

      // 2. Execute the CREATE TABLE statement
      logInfo(s"Executing SQL to create Iceberg table: $tableName")

      spark.sql(
        s"""
          |CREATE TABLE IF NOT EXISTS $tableName (
          |  $ddlSchema
          |)
          |USING iceberg
          |PARTITIONED BY (days(timestamp))
          |TBLPROPERTIES (
          |  'write.format.default'='parquet'
          |)
        """.stripMargin
      )

      logInfo(s"Successfully created/verified table $tableName")

    } catch {
      case e: Exception =>
        logError(s"Failed to create table $tableName", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
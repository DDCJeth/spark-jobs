import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.io.Source

object LoadToIcebergGenv1 {

  def main(args: Array[String]): Unit = {
    // Expected args: <inputPath> <tableName> <schemaPath> <partitionCol>
    if (args.length < 3) {
      println("Usage: LoadToIceberg <inputPath> <tableName> <schemaPath> [partitionCol]")
      sys.exit(1)
    }

    val inputPath    = args(0)
    val targetTable  = args(1)
    val schemaPath   = args(2) 
    val partitionCol = if (args.length > 3) args(3) else "timestamp"

    val spark = SparkSession.builder()
      .appName(s"Validated Ingest: $targetTable")
      .getOrCreate()

    // 1. Load and Parse External Schema
    // If schema is on S3, use spark.sparkContext.textFile. If local, use scala.io
    val schemaJson = spark.sparkContext.textFile(schemaPath).collect().mkString
    val customSchema = StructType.fromJson(schemaJson)

    // 2. Read CSV with Strict Schema
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST") // Stop immediately if data doesn't match schema
      .schema(customSchema)
      .csv(inputPath)

    // 3. Validation and Transformation
    val enrichedDf = df.withColumn("ingestion_date", current_date())

    // 4. Create Namespace and Table
    val namespace = targetTable.split("\\.")(0)
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")

    // Use 'createOrReplace' to ensure Iceberg table matches our JSON schema exactly
    enrichedDf.writeTo(targetTable)
      .tableProperty("write.format.default", "parquet")
      .partitionedBy(days(col(partitionCol)))
      .createOrLink()

    enrichedDf.writeTo(targetTable).append()

    spark.stop()
  }
}
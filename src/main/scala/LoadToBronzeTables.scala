import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._
import scala.io.Source

object LoadToBronzeTables {

  def main(args: Array[String]): Unit = {
    // Expected args: <inputPath> <tableName> <schemaPath> <partitionCol>
    // Expected args: 1. inputPath (e.g. s3://datalake/logType/) 
    //                2. logType (e.g. voice ; data ; sms )
    //                3. tableName (e.g. cdr.voice)

    if (args.length < 3) {
      println("Usage: LoadToBronzeTables <inputPath> <logType> <tableName>")
      sys.exit(1)
    }

    val inputPath    = args(0) // e.g. s3://datalake/logType/
    val logType      = args(1) // e.g. voice ; data ; sms
    val targetTable  = args(2) // e.g. cdr.voice

    val spark = SparkSession.builder()
      .appName(s"Validated Ingest: $targetTable")
      .getOrCreate()

    // 1. Load and Parse External Schema
    // If schema is on S3, use spark.sparkContext.textFile. If local, use scala.io
    val schemaBasePath = "s3a://datalake/schemas/"
    val schemaPath     = s"$schemaBasePath/${logType}_schema.json"
    val schemaJson = spark.sparkContext.textFile(schemaPath).collect().mkString
    val customSchema = DataType.fromJson(schemaJson).asInstanceOf[StructType]

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

    enrichedDf.writeTo(targetTable).append()

    spark.stop()
  }
}
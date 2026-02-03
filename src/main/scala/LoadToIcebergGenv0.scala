import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object LoadToIcebergGenv0 extends Logging {

  def main(args: Array[String]): Unit = {
    
    // 1. Parameterize inputs via CLI args or Spark Conf
    // Expected args: <inputPath> <tableName> <partitionColumn>
    if (args.length < 2) {
      logError("Usage: LoadToIceberg <inputPath> <tableName> [partitionColumn]")
      sys.exit(1)
    }

    val inputPath       = args(0)
    val targetTable     = args(1) // e.g., "cdr.data_logs"
    val partitionColumn = if (args.length > 2) args(2) else "timestamp"
    val namespace       = targetTable.split("\\.")(0)

    val spark = SparkSession.builder()
      .appName(s"Ingest to $targetTable")
      .getOrCreate()

    // ----------------------------------------------------
    // 1. Generic Read (Schema on Read)
    // ----------------------------------------------------
    logInfo(s"Reading data from $inputPath")
    
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // Add metadata columns
    val enrichedDf = df
      .withColumn("ingestion_date", current_date())

    // ----------------------------------------------------
    // 2. Generic Table Creation
    // ----------------------------------------------------
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")

    // Create table using 'createOrReplace' or 'createTable' syntax 
    // to automatically derive schema from the DataFrame
    logInfo(s"Ensuring table $targetTable exists")
    
    enrichedDf.writeTo(targetTable)
      .tableProperty("write.format.default", "parquet")
      // Use 'partitionedBy' to make partitioning generic
      .partitionedBy(days(col(partitionColumn)))
      .createOrLink() 

    // ----------------------------------------------------
    // 3. Append Data
    // ----------------------------------------------------
    logInfo(s"Appending data to $targetTable")
    
    enrichedDf
        .writeTo(targetTable)
        .append()

    spark.stop()
  }
}
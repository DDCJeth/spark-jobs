import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("K8sSparkApp")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(("Kubernetes", 1), ("Spark", 2), ("Operator", 3)).toDF("word", "count")
    
    data.show()
    spark.stop()
  }
}
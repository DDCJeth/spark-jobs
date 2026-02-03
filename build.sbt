name := "spark-iceberg-app"
version := "1.0"
scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val icebergVersion = "1.10.0"
val hadoopVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.iceberg" %% "iceberg-spark-runtime-4.0" % icebergVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion
)

assembly / assemblyJarName := "app.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
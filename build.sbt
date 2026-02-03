name := "spark-iceberg-minio"
version := "0.1.0"
scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val icebergVersion = "1.5.2" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  
  // Iceberg Spark Runtime
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,

  // The specific AWS/Iceberg bundles you requested
  // Note: software.amazon.awssdk is the v2 SDK used by modern Iceberg
  "software.amazon.awssdk" % "bundle" % "2.23.19",
  "org.apache.iceberg"    % "iceberg-aws-bundle" % "1.5.2" // Match your Iceberg version for stability
)

assembly / assemblyJarName := "app.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
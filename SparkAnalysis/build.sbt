name := "SparkAnalysis"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "com.google.guava" % "guava" % "15.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.5",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0",
  "org.apache.hbase" % "hbase-common" % "1.0.0",
  "org.apache.hbase" % "hbase-client" % "1.0.0",
  "org.apache.hbase" % "hbase-server" % "1.0.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

dependencyOverrides += "com.google.guava" % "guava" % "15.0"
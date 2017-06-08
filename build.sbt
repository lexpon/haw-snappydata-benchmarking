name := "haw-snappydata-benchmarking"

version := "1.0"

scalaVersion := "2.11.7"

// Apache Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"

// SnappyData
libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "0.8"
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "0.8"

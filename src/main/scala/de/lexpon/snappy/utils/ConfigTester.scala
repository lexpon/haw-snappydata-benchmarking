package de.lexpon.snappy.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import java.io.PrintWriter
import scala.collection.JavaConversions._

object ConfigTester
{
    def main(args: Array[String]): Unit =
    {
        println("Hello, world!")

        val conf = ConfigFactory.load()
        val schemaFilePathList = conf.getStringList("schema-files")
        schemaFilePathList.toList.toStream
            .foreach(configValue => println("filepath: {" + configValue + "}"))

        val sqlFilePathList = List(
            "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_01_drop_tables.sql",
            "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_02_create_tables.sql",
            "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_source_01_drop_tables.sql",
            "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_source_02_create_tables.sql",
            "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_ri.sql"
        )
        sqlFilePathList.toStream
            .foreach(filePath => println("filepath: {" + filePath + "}"))
    }
}

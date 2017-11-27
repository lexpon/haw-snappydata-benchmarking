package de.lexpon.snappy.utils

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import java.io.PrintWriter
import scala.collection.JavaConversions._

object ConfigTester
{
    def main(args: Array[String]): Unit =
    {
        //        schemaFiles()
        csvFiles()
    }


    private def schemaFiles() =
    {
        println("Schema Files!")

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


    private def csvFiles() =
    {
        println("CSV Files!")

        val conf = ConfigFactory.load()
        val csvFileToInsertList = conf.getConfig("csv-files-to-insert")
        val baseFolder = csvFileToInsertList.getString("base-folder")

        println("base folder: " + baseFolder)

        csvFileToInsertList.getConfigList("file-list").get(0).getString("file")

        val fileList = csvFileToInsertList.getConfigList("file-list")
        fileList.toStream
            .foreach(fileEntry =>
            {
                val filePath: String = baseFolder + fileEntry.getString("file")
                val tableName: String = fileEntry.getString("table")

                println("try to load data from file '" + filePath + "' into table '" + tableName + "'")
            })
    }
}

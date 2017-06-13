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
        val csvFileToInsertList = conf.getObject("csv-files-to-insert")
        val baseFolder: String = csvFileToInsertList.get("base-folder").render()

        println("base folder: " + baseFolder)

        val fileList: ConfigList = conf.getObject("csv-files-to-insert").get("file-list").asInstanceOf[ConfigList]
        fileList.toStream
            .foreach(file =>
            {
                val fileConfig: ConfigObject = file.asInstanceOf[ConfigObject]
                val fileName: String = fileConfig.get("file").render()
                val tableName: String = fileConfig.get("table").render()
                println("file name:  " + fileName)
                println("table name: " + tableName)
            })
    }
}

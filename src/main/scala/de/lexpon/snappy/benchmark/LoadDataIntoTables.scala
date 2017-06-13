package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import java.io.{File, PrintWriter}

class LoadDataIntoTables extends SnappySQLJob
{
    private val config: Config = ConfigFactory.load()
    private val sqlDelimiter: String = ";"


    private def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        loadDataIntoDataStore(snappySession)
    }


    private def loadDataIntoDataStore(snappySession: SnappySession): Any =
    {
        val pw = new PrintWriter("csv_to_dataframe_to_snappystore.out")

        val csvFileToInsertList = config.getConfig("csv-files-to-insert")
        val baseFolder = csvFileToInsertList.getString("base-folder")

        val fileList = csvFileToInsertList.getConfigList("file-list")
        fileList.toStream
            .foreach(fileEntry =>
            {
                val filePath: String = baseFolder + fileEntry.getString("file")
                val tableName: String = fileEntry.getString("table")

                pw.println("try to load data from file '" + filePath + "' into table '" + tableName + "'")
                loadDataIntoTable(snappySession, pw, filePath, tableName)
            })

        pw.close()
    }


    private def loadDataIntoTable(snappySession: SnappySession, pw: PrintWriter, filePath: String, tableName: String) =
    {
        val tableSchema = snappySession.table(tableName).schema
        val customerDF = snappySession.read
            .schema(schema = tableSchema)
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", "|")
            .csv(filePath)

        pw.println("table schema for table '" + tableName + "':")
        pw.println(tableSchema.toString())

        pw.println("finished reading csv file '" + filePath + "' as a dataframe")
        pw.println("dataframe:")
        pw.println(customerDF.show())
        pw.println()

        pw.println("try to save the dataframe to table '" + tableName + "'")
        customerDF.write.insertInto(tableName)
    }
}

package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import java.io.{File, PrintWriter, FileWriter, BufferedWriter}

class LoadDataIntoTables extends SnappySQLJob
{
    private val config: Config = ConfigFactory.load()
    private val sqlDelimiter: String = ";"


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        loadDataIntoDataStore(snappySession)
    }


    private def loadDataIntoDataStore(snappySession: SnappySession): Any =
    {

        val csvFileToInsertList = config.getConfig("csv-files-to-insert")
        val baseFolder = csvFileToInsertList.getString("base-folder")

        val pw = new PrintWriter("csv_to_dataframe_to_snappystore.out")
        pw.println("Try to load these csv files into tables: " + csvFileToInsertList.toString)
        pw.close()

        val pwErr = new PrintWriter("csv_to_dataframe_to_snappystore_error.out")
        pwErr.println("Prepare file for logging errors...")
        pwErr.close()

        val fileList = csvFileToInsertList.getConfigList("file-list")
        fileList.toStream
            .foreach(fileEntry =>
            {
                val filePath: String = baseFolder + fileEntry.getString("file")
                val tableName: String = fileEntry.getString("table")

                writeMsgToLog("try to load data from file '" + filePath + "' into table '" + tableName + "'")

                try
                {
                    loadDataIntoTable(snappySession, filePath, tableName)
                }
                catch
                {
                    case e: Exception =>
                    {
                        writeErrToLog("Exception occured: " + e.getMessage +
                            " - Exception: " + e.toString)
                    }
                }
            })
    }


    private def writeMsgToLog(msg: String) =
    {
        val fw = new FileWriter("csv_to_dataframe_to_snappystore.out", true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(msg)
        out.close()
    }


    private def writeErrToLog(err: String) =
    {
        val fw = new FileWriter("csv_to_dataframe_to_snappystore_error.out", true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(err)
        out.close()
    }


    private def loadDataIntoTable(snappySession: SnappySession, filePath: String, tableName: String) =
    {
        val tableSchema = snappySession.table(tableName).schema
        val customerDF = snappySession.read
            .schema(schema = tableSchema)
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", "|")
            .csv(filePath)

        try
        {
            customerDF.write.insertInto(tableName)
        }
        catch
        {
            case e: Exception => writeErrToLog("could not write dataframe to table. table: "
                + tableName + "; dataframe: " + customerDF.toString() + "; Exception: " + e)
        }
    }
}

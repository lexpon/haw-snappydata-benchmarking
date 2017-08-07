package de.lexpon.snappy.benchmark

import com.typesafe.config.{Config, _}
import java.io.{BufferedWriter, FileWriter, PrintWriter}
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}
import scala.collection.JavaConversions._

class LoadDataIntoTables extends SnappySQLJob
{
    private val config: Config = ConfigFactory.load()
    private val sqlDelimiter: String = ";"

    private val fileNameLog: String = "LoadDataIntoTablesSnappyJob_LOG.out";
    private val fileNameErr: String = "LoadDataIntoTablesSnappyJob_ERR.out";


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        loadDataIntoDataStore(snappySession)
    }


    private def loadDataIntoDataStore(snappySession: SnappySession): Any =
    {

        val csvFileToInsertList = config.getConfig("csv-files-to-insert")
        val baseFolder = csvFileToInsertList.getString("base-folder")

        val pw = new PrintWriter(fileNameLog)
        pw.println("Try to load these csv files into tables: " + csvFileToInsertList.toString)
        pw.close()

        val pwErr = new PrintWriter(fileNameErr)
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
        val fw = new FileWriter(fileNameLog, true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(msg)
        out.close()
    }


    private def writeErrToLog(err: String) =
    {
        val fw = new FileWriter(fileNameErr, true)
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

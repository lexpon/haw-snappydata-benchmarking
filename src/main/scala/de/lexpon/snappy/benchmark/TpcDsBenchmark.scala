package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import java.io.{File, PrintWriter}

class TpcDsBenchmark extends SnappySQLJob
{
    private val config: Config = ConfigFactory.load()
    private val sqlDelimiter: String = ";"


    private def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        createSchema(snappySession)

        loadDataIntoDataStore(snappySession)
    }


    private def createSchema(snappySession: SnappySession) =
    {
        val schemaFilePathList = config.getStringList("schema-files")
        schemaFilePathList.toList.toStream
            .foreach(filePath => runSqlStatementsFromFile(snappySession, filePath))
    }


    private def runSqlStatementsFromFile(snappySession: SnappySession, sqlFilePath: String): Any =
    {
        val source = fromFile(sqlFilePath)
        val fileContent = try source.mkString
        finally source.close()

        val createSqlStatements: Array[String] = fileContent.split(sqlDelimiter)

        createSqlStatements.toStream
            .map(statement => statement.trim)
            .filter(statement => !statement.isEmpty)
            .foreach(statement => snappySession.sql(statement))
    }


    private def loadDataIntoDataStore(snappySession: SnappySession): Any =
    {
        val pw = new PrintWriter("csv_to_dataframe_to_snappystore.out")

        val tableSchema = snappySession.table("call_center").schema
        val customerDF = snappySession.read
            .schema(schema = tableSchema)
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", "|")
            .csv("/Users/lexpon/benchmarks/tpcds/0001_GB/call_center.dat")

        pw.println()
        pw.println("tableSchema:")
        pw.println(tableSchema.toString())

        pw.println()
        pw.println("finished reading csv file as a df")
        pw.println()
        pw.println("dataframe:")
        pw.println()
        pw.println(customerDF.show())
        pw.println()

        pw.println("try to save the df to table")
        customerDF.write.insertInto("call_center")

        pw.close()
    }

}

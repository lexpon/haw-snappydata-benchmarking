package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import java.io.PrintWriter
import scala.collection.JavaConversions._

class TpcDsBenchmark extends SnappySQLJob
{
    val config: Config = ConfigFactory.load()
    val sqlDelimiter: String = ";"


    def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        createSchema(snappySession)
    }


    private def createSchema(snappySession: SnappySession) =
    {
        val schemaFilePathList = config.getStringList("schema-files")
        schemaFilePathList.toList.toStream
            .foreach(filePath => runSqlStatement(snappySession, filePath))
    }


    def runSqlStatement(snappySession: SnappySession, sqlFilePath: String): Any =
    {
        val source = scala.io.Source.fromFile(sqlFilePath)
        val fileContent = try source.mkString
        finally source.close()

        val createSqlStatements: Array[String] = fileContent.split(sqlDelimiter)

        createSqlStatements.toStream
            .map(statement => statement.trim)
            .filter(statement => !statement.isEmpty)
            .foreach(statement => snappySession.sql(statement))
    }
}

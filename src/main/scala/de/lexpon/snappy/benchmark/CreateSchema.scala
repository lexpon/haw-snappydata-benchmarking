package de.lexpon.snappy.benchmark

import de.lexpon.snappy.benchmark.RunSqlStringStatement
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import java.io.{File, PrintWriter}

class CreateSchema extends SnappySQLJob
{
    private val config: Config = ConfigFactory.load()
    private val sqlDelimiter: String = ";"


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        createSchema(snappySession)
    }


    private def createSchema(snappySession: SnappySession) =
    {
        val schemaFilePathList = config.getStringList("schema-files")
        schemaFilePathList.toList.toStream
            .foreach(filePath => RunSqlStringStatement.runSqlStatementsFromFile(snappySession, filePath, sqlDelimiter))
    }
}

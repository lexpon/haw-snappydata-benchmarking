package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import java.io.{File, PrintWriter}

object RunSqlStringStatement
{
    def runSqlStatementsFromFile(snappySession: SnappySession, sqlFilePath: String, sqlDelimiter: String): Any =
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
}

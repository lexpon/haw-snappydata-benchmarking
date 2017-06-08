package de.lexpon.snappy.benchmark

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class CreateTables extends SnappySQLJob
{
    val filePathDropTables: String = "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_01_drop_tables.sql"
    val filePathCreateTables: String = "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds_02_create_tables.sql"
    val sqlDelimiter: String = ";"


    def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath


    override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()


    override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any =
    {
        runSqlStatement(snappySession, filePathDropTables)
        runSqlStatement(snappySession, filePathCreateTables)
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

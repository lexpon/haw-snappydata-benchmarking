package de.lexpon.snappy.benchmark

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object CreateTables extends SnappySQLJob {
    def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

    override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {
        createTables()
    }

    private def createTables() = {
        val pw = new PrintWriter("CreateTables.out")
        pw.println()
        pw.println("**** Creating benchmark tables using SQL ****")

        val filepath: String = "/Users/lexpon/benchmarks/tpcds/0001_GB/tpcds.sql"
        pw.println()
        pw.println("--- Read file ---" + filepath)
        val source = scala.io.Source.fromFile(filepath)
        val lines = try source.mkString finally source.close()
        pw.println("--- This is the file content: ---")
        pw.println(lines)

        pw.println()
        pw.println("**** Done ****")
        pw.close()
    }
}

package de.lexpon.snappy.benchmark

class ClearSqlQueriesToRun
{

    import java.io.{BufferedWriter, FileWriter, PrintWriter}
    import org.apache.spark.sql.{CachedDataFrame, SnappySession, SparkSession}
    import scala.collection.mutable.ListBuffer
    import scala.io.Source

    val logFileName: String = "RunSqlQueriesZeppelin_LOG.out"
    val errFileName: String = "RunSqlQueriesZeppelin_ERR.out"
    val benchmarkFileName: String = "/home/snappydata/benchmarks/tpcds/queries/benchmark_queries.sql"
    val clearedBenchmarkFile: String = "/home/snappydata/benchmarks/tpcds/queries/benchmark_queries_cleared.sql"

    val spark: SparkSession = SparkSession
        .builder
        .appName("LoadDataIntoTables")
        .master("local[*]")
        .getOrCreate

    val snappySession: SnappySession = new SnappySession(spark.sparkContext)


    def clearLogs(): Unit =
    {
        val fileNames = List(logFileName, errFileName, clearedBenchmarkFile)
        fileNames.foreach(fileName =>
        {
            val fw = new FileWriter(fileName, false)
            val bw = new BufferedWriter(fw)
            val out = new PrintWriter(bw)
            out.println("")
            out.close()
        })
    }


    def writeMsgToLog(msg: String): Unit =
    {
        println(msg)
        val fw = new FileWriter(logFileName, true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(msg)
        out.close()
    }


    def writeErrToLog(err: String): Unit =
    {
        println(err)
        val fw = new FileWriter(errFileName, true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(err)
        out.close()
    }


    def findSqlQueriesInFile(snappySession: SnappySession, benchmarkFileName: String): List[String] =
    {
        val lines = Source.fromFile(benchmarkFileName).getLines()
        val queryBuffer: ListBuffer[String] = new ListBuffer()
        var query: StringBuffer = new StringBuffer()

        lines.foreach(line =>
        {
            if (line.startsWith("--BEGIN: QUERY"))
            {
                query = new StringBuffer()
            }
            query.append(line)
            query.append("\n")
            if (line.startsWith("-- END: QUERY"))
            {
                queryBuffer.+=(query.toString)
            }
        })

        queryBuffer.toList
    }


    def runSqlQuery(snappySession: SnappySession, query: String): Option[String] =
    {
        val queriesWithoutExceptions: ListBuffer[String] = new ListBuffer()
        try
        {
            val result: CachedDataFrame = snappySession.sql(query)
            if (result.count() > 0)
            {
                writeMsgToLog("this is the result of the execution: " + result)
                return Some(query)
            }
        }
        catch
        {
            case e: Exception =>
                writeErrToLog("this query lead to an exception: " + query)
        }
        None
    }


    def printSqlQueriesToFile(queries: List[String], filename: String): Unit =
    {
        writeMsgToLog("These are the cleared queries: " + queries)
        val fw = new FileWriter(filename, false)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        queries.foreach(query => out.println(query))
        out.close()
    }


    clearLogs()
    val sqlQueryList: List[String] = findSqlQueriesInFile(snappySession, benchmarkFileName)
    //writeMsgToLog("querys to execute: " + sqlQueryList.toString())
    val queriesWithoutExceptions: List[String] = sqlQueryList
        .flatMap(query => runSqlQuery(snappySession, query))
    writeMsgToLog("executed queries: " + queriesWithoutExceptions.toString())
    printSqlQueriesToFile(queriesWithoutExceptions, clearedBenchmarkFile)

}

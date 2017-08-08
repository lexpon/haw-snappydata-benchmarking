package de.lexpon.snappy.benchmark

class RunSqlQueriesAndMeasureRunTimeZeppelin
{

    import java.io.{BufferedWriter, FileWriter, PrintWriter}
    import org.apache.spark.sql.{CachedDataFrame, SnappySession, SparkSession}
    import scala.collection.mutable.ListBuffer
    import scala.io.Source

    val logFileName: String = "RunSqlQueriesAndMeasureRunTimeZeppelin_LOG.out"
    val errFileName: String = "RunSqlQueriesAndMeasureRunTimeZeppelin_ERR.out"
    val benchmarkFileName: String = "/home/snappydata/benchmarks/tpcds/queries/benchmark_queries_cleared.sql"

    val spark: SparkSession = SparkSession
        .builder
        .appName("LoadDataIntoTables")
        .master("local[*]")
        .getOrCreate

    val snappySession: SnappySession = new SnappySession(spark.sparkContext)


    def clearLogs(): Unit =
    {
        val fileNames = List(logFileName, errFileName)
        fileNames.foreach(fileName =>
        {
            val fw = new FileWriter(fileName, false)
            val bw = new BufferedWriter(fw)
            val out = new PrintWriter(bw)
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


    def parseQueryNumber(query: String): String =
    {
        query.lines.foreach(line =>
        {
            if (line.startsWith("--BEGIN: QUERY"))
            {
                return line.substring(line.length - 2, line.length)
            }
        })
        writeErrToLog("could not parse query number from query: " + query)
        ""
    }


    def runSqlQuery(snappySession: SnappySession, query: String): Unit =
    {
        val queriesWithoutExceptions: ListBuffer[String] = new ListBuffer()
        try
        {
            val queryNumber: String = parseQueryNumber(query)

            val t0 = System.nanoTime()
            val result: CachedDataFrame = snappySession.sql(query)
            val t1 = System.nanoTime()

            val ns = t1 - t0
            val ms = ns / 1000000
            val s = ms / 1000
            writeMsgToLog(queryNumber + "; " + ns + "; " + ms + "; " + s)
        }
        catch
        {
            case e: Exception =>
                writeErrToLog("this query lead to an exception: " + query)
        }
    }


    clearLogs()
    writeMsgToLog("query number; run time in ns; run time in ms; run time in s")
    val sqlQueryList: List[String] = findSqlQueriesInFile(snappySession, benchmarkFileName)
    sqlQueryList.foreach(query =>
    {
        runSqlQuery(snappySession, query)
    })
}

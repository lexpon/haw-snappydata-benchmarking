package de.lexpon.snappy.benchmark

class RunSqlQueriesAndMeasureRunTimeZeppelin
{

    import java.io.{BufferedWriter, FileWriter, PrintWriter}
    import org.apache.spark.sql.{CachedDataFrame, SnappySession, SparkSession}
    import scala.collection.mutable.ListBuffer
    import scala.io.Source

    val benchmarkFileName: String = "/home/snappydata/benchmarks/tpcds/queries/benchmark_queries_cleared.sql"

    val spark: SparkSession = SparkSession
        .builder
        .appName("LoadDataIntoTables")
        .master("local[*]")
        .getOrCreate

    val snappySession: SnappySession = new SnappySession(spark.sparkContext)


    def clearLogs(roundNumber: Int): Unit =
    {
        val fileNames = List("measure_" + roundNumber + ".csv", "measure_" + roundNumber + "_err.csv")
        fileNames.foreach(fileName =>
        {
            val fw = new FileWriter(fileName, false)
            val bw = new BufferedWriter(fw)
            val out = new PrintWriter(bw)
            out.close()
        })
    }


    def writeMsgToLog(msg: String, roundNumber: Int): Unit =
    {
        println(msg)
        val fw = new FileWriter("measure_" + roundNumber + ".csv", true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(msg)
        out.close()
    }


    def writeErrToLog(err: String, roundNumber: Int): Unit =
    {
        println(err)
        val fw = new FileWriter("measure_" + roundNumber + "_err.csv", true)
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


    def parseQueryNumber(query: String, roundNumber: Int): String =
    {
        query.lines.foreach(line =>
        {
            if (line.startsWith("--BEGIN: QUERY"))
            {
                return line.substring(line.length - 2, line.length)
            }
        })
        writeErrToLog("could not parse query number from query: " + query, roundNumber)
        ""
    }


    def runSqlQuery(snappySession: SnappySession, query: String, roundNumber: Int): Unit =
    {
        try
        {
            val queryNumber: String = parseQueryNumber(query, roundNumber)

            val t0 = System.nanoTime()
            val result: CachedDataFrame = snappySession.sql(query)
            val t1 = System.nanoTime()

            val ns = t1 - t0
            val ms = ns / 1000000
            val s = ms / 1000
            writeMsgToLog(queryNumber + "; " + ns + "; " + ms + "; " + s, roundNumber)
        }
        catch
        {
            case e: Exception =>
                writeErrToLog("this query lead to an exception: " + query, roundNumber)
                writeErrToLog("exception: " + e.toString, roundNumber
                )
        }
    }


    def runOneMeasureRound(roundNumber: Int): Unit =
    {
        clearLogs(roundNumber)
        writeMsgToLog("query number; run time in ns; run time in ms; run time in s", roundNumber)
        val sqlQueryList: List[String] = findSqlQueriesInFile(snappySession, benchmarkFileName)
        sqlQueryList.foreach(query =>
        {
            runSqlQuery(snappySession, query, roundNumber)
        })
    }


    val numOfRounds: Int = 10
    var curRound: Int = 1
    while (curRound <= numOfRounds)
    {
        println("currentMeasureRound: " + curRound)
        runOneMeasureRound(curRound)
        curRound = curRound + 1
    }
}

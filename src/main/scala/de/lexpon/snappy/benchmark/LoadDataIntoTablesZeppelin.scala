package de.lexpon.snappy.benchmark

object LoadDataIntoTablesZeppelin
{

    import java.io.{BufferedWriter, FileWriter, PrintWriter}
    import org.apache.spark.sql.{SnappySession, SparkSession}
    import scala.collection.mutable.LinkedHashMap

    /*
        make definitions
    */
    val baseFolder: String = "/home/snappydata/benchmarks/tpcds/0001_GB/"

    val fileMap: LinkedHashMap[String, String] = LinkedHashMap(
        "call_center.dat" -> "call_center",
        "catalog_page.dat" -> "catalog_page",
        "catalog_returns.dat" -> "catalog_returns",
        "catalog_sales.dat" -> "catalog_sales",
        "customer.dat" -> "customer",
        "customer_address.dat" -> "customer_address",
        "customer_demographics.dat" -> "customer_demographics",
        "date_dim.dat" -> "date_dim",
        "dbgen_version.dat" -> "dbgen_version",
        "household_demographics.dat" -> "household_demographics",
        "income_band.dat" -> "income_band",
        "inventory.dat" -> "inventory",
        "item.dat" -> "item",
        "promotion.dat" -> "promotion",
        "reason.dat" -> "reason",
        "ship_mode.dat" -> "ship_mode",
        "store.dat" -> "store",
        "store_returns.dat" -> "store_returns",
        "store_sales.dat" -> "store_sales",
        "time_dim.dat" -> "time_dim",
        "warehouse.dat" -> "warehouse",
        "web_page.dat" -> "web_page",
        "web_returns.dat" -> "web_returns",
        "web_sales.dat" -> "web_sales",
        "web_site.dat" -> "web_site"
    )

    val spark: SparkSession = SparkSession
        .builder
        .appName("LoadDataIntoTables")
        .master("local[*]")
        .getOrCreate

    val snappySession: SnappySession = new SnappySession(spark.sparkContext)


    /*
        necessary methods
     */
    def writeMsgToLog(msg: String): Unit =
    {
        println(msg)
        val fw = new FileWriter("LoadDataIntoTablesZeppelin_LOG.out", true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(msg)
        out.close()
    }


    def writeErrToLog(err: String): Unit =
    {
        println(err)
        val fw = new FileWriter("LoadDataIntoTablesZeppelin_ERROR.out", true)
        val bw = new BufferedWriter(fw)
        val out = new PrintWriter(bw)
        out.println(err)
        out.close()
    }


    def loadDataIntoTable(snappySession: SnappySession, filePath: String, tableName: String): Unit =
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
            val dataframe = customerDF.write
            writeMsgToLog("customerDF.write works. will now try dataframe.insertInto(" + tableName + ")")
            dataframe.insertInto(tableName)
            writeMsgToLog("dataframe.insertInto(" + tableName + ") worked")
        }
        catch
        {
            case e: Exception => writeErrToLog("could not write dataframe to table. table: " + tableName + "; dataframe: " + customerDF
                .toString() + "; Exception: " + e)
        }
    }


    def loadDataIntoDataStore(
        snappySession: SnappySession,
        baseFolder: String,
        fileMap: LinkedHashMap[String, String]): Any =
    {
        fileMap.foreach
        { keyVal =>
            val filePath: String = baseFolder + keyVal._1
            val tableName: String = keyVal._2

            writeMsgToLog("try to load data from file '" + filePath + "' into table '" + tableName + "'")

            try
            {
                loadDataIntoTable(snappySession, filePath, tableName)
            }
            catch
            {
                case e: Exception => writeErrToLog("Exception occured: " + e.getMessage + " - Exception: " + e.toString)
            }
        }
    }


    /*
        RUN THE JOB
    */
    loadDataIntoDataStore(snappySession, baseFolder, fileMap)
}

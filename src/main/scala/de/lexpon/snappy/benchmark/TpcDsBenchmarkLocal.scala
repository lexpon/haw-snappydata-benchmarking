package de.lexpon.snappy.benchmark

import org.apache.spark.sql.{SparkSession, SnappySession, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object TpcDsBenchmarkLocal
{
    def main(args: Array[String]): Unit =
    {
        connectToSnappy()
    }


    private def connectToSnappy(): Any =
    {
        // https://stackoverflow.com/questions/44379913/snappydata-unable-to-access-snappydata-store-from-an-existing-spark-installation
        // http://snappydatainc.github.io/snappydata/howto/#how-to-access-snappydata-store-from-an-existing-spark-installation-using-smart-connector
        // http://snappydatainc.github.io/snappydata/deployment/

        //        val spark: SparkSession = SparkSession
        //            .builder
        //            .appName("TpcDsBenchmarkLocal")
        //            // It can be any master URL
        //            .master("local[*]")
        //            // snappydata.store.locators property enables the application to interact with SnappyData store
        //            .config("snappydata.store.locators", "localhost:10334")
        //            .getOrCreate
        //
        //
        //        val snSession = new SnappySession(spark.sparkContext)

        //        val spark = SparkSession
        //            .builder()
        //            .appName("SmartConnectorMainJava")
        //            .master("local[*]")
        //            .config("snappydata.store.locators", "localhost:10334")
        //            .getOrCreate()
        //        val snSession = new SnappySession(spark.sparkContext)

        //        import java.sql.Connection
        //        import java.sql.DriverManager
        //        val c = DriverManager.getConnection("jdbc:snappydata://locatorHostName:1527/")

        //        import org.apache.spark.api.java.JavaSparkContext
        //        import org.apache.spark.sql.SnappySession
        //        import org.apache.spark.sql.SparkSession
        //        val spark = SparkSession.builder
        //            .appName("TpcDsBenchmarkLocal")
        //            .master("local[*]")
        //            .config("snappydata.store.locators", "localhost:10334")
        //            .getOrCreate
        //        val jsc = new JavaSparkContext(spark.sparkContext)
        //        val snSession = new SnappySession(spark.sparkContext)

        val spark: SparkSession = SparkSession
            .builder
            .appName("SparkApp")
            .master("local[*]")
            .config("snappydata.store.locators", "localhost:10334")
            .getOrCreate
        val snSession = new SnappySession(spark.sparkContext)

        val tableSchema = snSession.table("call_center").schema
        val customerDF = snSession.read.schema(schema = tableSchema)
            .csv("/Users/lexpon/benchmarks/tpcds/0001_GB/call_center.dat")
    }
}

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
        val spark: SparkSession = SparkSession
            .builder
            .appName("ConnectToSnappy")
            .master("local[*]")
            //            .config("spark.snappydata.store.locators", "localhost:10334")
            .getOrCreate
        val snappySession = new SnappySession(spark.sparkContext)

        val tableSchema = snappySession.table("call_center").schema
        val customerDF = snappySession.read.schema(schema = tableSchema)
            .csv("/Users/lexpon/benchmarks/tpcds/0001_GB/call_center.dat")
    }
}

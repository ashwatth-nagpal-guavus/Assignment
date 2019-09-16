package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
object Question5 {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .appName("TopSubscribers")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._


      val ggsnXMLDF = spark.read
        .option("rowTag", "ggsn")
        .option("valueTag", "v")
        .xml("/tmp/ashwatth/ggsn.xml")

      val ggsnFormattedDF = ggsnXMLDF
        .select($"_name", explode($"rule"))
        .select($"_name", $"col.condition._value")
        .toDF("name", "ggsnip")


      val edrHttpLogsDF = spark.table("ashwatth.edr_http_logs")

      val joinedDF = edrHttpLogsDF
        .select(
          "ggsnip",
          "transactionuplinkbytes",
          "transactiondownlinkbytes",
          "hour"
        )
        .filter($"ggsnip".isNotNull)
        .join(ggsnFormattedDF, Seq("ggsnip"))

      joinedDF
        .groupBy("hour", "name")
        .agg(
          (sum("transactionuplinkbytes") + sum("transactiondownlinkbytes"))
            .as("Tonnage")
        )
        .write
        .partitionBy("hour")
        .format("orc")
        .saveAsTable("ashwatth.question5")
    } catch {
      case ex: Exception => println(ex)
    }
  }
}

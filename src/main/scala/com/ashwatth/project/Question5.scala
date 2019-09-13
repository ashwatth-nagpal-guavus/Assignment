package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
        .format("xml")
        .load("/tmp/ashwatth/ggsn.xml")

      val ggsnFormattedDF = ggsnXMLDF
        .select($"_name", explode($"rule"))
        .select($"_name", $"col.condition._value")
        .toDF("name", "ggsnIp")

      val edrHttpLogsDF = spark.table("edr_http_logs")

      val joinedDF = edrHttpLogsDF
        .select(
          "ggsnIp",
          "transactionUplinkBytes",
          "transactionDownlinkBytes",
          "Hour"
        )
        .filter($"ggsnIp".isNotNull)
        .join(ggsnFormattedDF, Seq("ggsnIp"))

      joinedDF
        .groupBy("Hour", "name")
        .agg(
          (sum("transactionUplinkBytes") + sum("transactionDownlinkBytes"))
            .as("Tonnage")
        )
        .write
        .partitionBy("Hour")
        .format("orc")
        .saveAsTable("ashwatth.question5_ashwatth")
    } catch {
      case ex: Exception => println(ex)
    }
  }
}

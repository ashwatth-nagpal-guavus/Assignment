package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TopSubscribers")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val ggsnXMLDF = spark.read
      .option("rowTag", "ggsn")
      .format("xml")
      .load("../../ashwatth/EDR_Dataset/ggsn.xml")

    val ggsnFormattedDF = ggsnXMLDF
      .select($"_name", explode($"rule"))
      .map(x => (x.getString(0), x.getStruct(1).getStruct(0).getString(2)))
      .toDF("name", "ggsnIp")

    val smallSet = spark.read
      .orc("../../ashwatth/minhour/")

    val joinedDF = smallSet
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
      .orc("../../ashwatth/TonnagePerGgsnName")
  }
}

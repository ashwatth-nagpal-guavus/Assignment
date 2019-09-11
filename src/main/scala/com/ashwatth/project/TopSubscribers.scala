package com.ashwatth.project

import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.Window

object TopSubscribers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TopSubscribers")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val smallSet = spark.read
      .orc("../../ashwatth/minhour/")

    val dataPerUserPerHour = smallSet
      .groupBy("Hour", "radiusUserName")
      .agg(
        sum("transactionDownlinkBytes").as("total-download-bytes"),
        sum("transactionUplinkBytes")
          .as("total-upload-bytes"),
        (
          sum("transactionDownlinkBytes") + sum("transactionUplinkBytes")
        ).as("tonnage")
      )

    val downloadWindow =
      Window.partitionBy("Hour").orderBy(desc("total-download-bytes"))

    val uploadWindow =
      Window.partitionBy("Hour").orderBy(desc("total-upload-bytes"))

    val tonnageWindow = Window.partitionBy("Hour").orderBy(desc("tonnage"))

    val topSubscribersPerHourByDownLoadByte =
      dataPerUserPerHour
        .select("Hour", "radiusUserName", "total-download-bytes")
        .withColumn("rn", row_number.over(downloadWindow))
        .where($"rn" <= 10)
        .withColumnRenamed("radiusUserName", "byDownload")

    val top10SubscribersPerHourByUpLoadByte =
      dataPerUserPerHour
        .select("Hour", "radiusUserName", "total-upload-bytes")
        .withColumn("rn", row_number.over(uploadWindow))
        .where($"rn" <= 10)
        .withColumnRenamed("radiusUserName", "byUpload")

    val top10SubscribersPerHourByTonnage =
      dataPerUserPerHour
        .select("Hour", "radiusUserName", "tonnage")
        .withColumn("rn", row_number.over(tonnageWindow))
        .where($"rn" <= 10)
        .withColumnRenamed("radiusUserName", "byTonnage")

    topSubscribersPerHourByDownLoadByte
      .join(top10SubscribersPerHourByUpLoadByte, Seq("Hour", "rn"))
      .join(top10SubscribersPerHourByTonnage, Seq("Hour", "rn"))
      .select("Hour", "rn", "byDownload", "byUpload", "byTonnage")
      .orderBy(asc("Hour"), asc("rn"))
      .show(30, false)

  }

}

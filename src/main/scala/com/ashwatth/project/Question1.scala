package com.ashwatth.project

import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.Window

object Question1 {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .appName("TopSubscribers")
        .enableHiveSupport()
        .getOrCreate()

      import spark.implicits._

      val edrHttpLogsDF = spark.table("ashwatth.edr_http_logs")

      val dataPerUserPerHour = edrHttpLogsDF
        .groupBy("Hour", "radiusUserName")
        .agg(
          sum("transactionDownlinkBytes").as("total-download-bytes"),
          sum("transactionUplinkBytes")
            .as("total-upload-bytes"),
          (
            sum("transactionDownlinkBytes") + sum("transactionUplinkBytes")
          ).as("tonnage")
        )
      dataPerUserPerHour.persist()
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

      val top10Subscribers = topSubscribersPerHourByDownLoadByte
        .join(top10SubscribersPerHourByUpLoadByte, Seq("Hour", "rn"))
        .join(top10SubscribersPerHourByTonnage, Seq("Hour", "rn"))
        .select("Hour", "rn", "byDownload", "byUpload", "byTonnage")
        .orderBy(asc("Hour"), asc("rn"))

      top10Subscribers.write
        .partitionBy("Hour")
        .format("orc")
        .saveAsTable("ashwatth.question1")

    } catch {
      case ex: Exception => println(ex)
    }

  }

}

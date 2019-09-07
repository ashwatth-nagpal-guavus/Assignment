package com.ashwatth.project
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TopSubscribers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TopSubscribers")
      .getOrCreate()

    val smallSet = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("../../ashwatth/smallSet.csv")

    val dataPerIPAddress = smallSet
      .groupBy("radius-user-name")
      .agg(
        sum("transaction-downlink-bytes").as("total-download-bytes"),
        sum("transaction-uplink-bytes").as("total-upload-bytes")
      )

    val topSubscribersByDownLoadByte =
      dataPerIPAddress
        .select("radius-user-name", "total-download-bytes")
        .orderBy(desc("total-download-bytes"))

    val topSubscribersByUpLoadByte =
      dataPerIPAddress
        .select("radius-user-name", "total-upload-bytes")
        .orderBy(desc("total-upload-bytes"))

    println("Top 10 Subscriber as per download bytes ")

    topSubscribersByDownLoadByte.show(10)

    println("Top 10 Subscriber as per upload bytes ")

    topSubscribersByUpLoadByte.show(10)

    println("Top 10 Subscriber as per upload + download bytes ")

    dataPerIPAddress
      .select(
        dataPerIPAddress("radius-user-name"),
        (dataPerIPAddress("total-download-bytes") + dataPerIPAddress(
          "total-upload-bytes"
        )).as("sum(up+down)")
      )
      .orderBy(desc("sum(up+down)"))
      .show(10)

  }

}

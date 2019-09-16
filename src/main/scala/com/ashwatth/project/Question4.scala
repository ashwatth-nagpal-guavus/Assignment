package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("TopSubscribers")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val edrHttpLogs = spark.table("ashwatth.edr_http_logs")
    val contentTypeDF =
      spark.read.csv("/tmp/ashwatth/tempMap").toDF("httpcontenttype", "type")
    val joinedDf = edrHttpLogs.join(contentTypeDF, Seq("httpcontenttype"))

    val validUrl =
      joinedDf
        .filter($"httpurl".isNotNull && $"httpreplycode".rlike("2[0-9][0-9]"))
        .filter(eachRow => checkUrl(eachRow.getString(11)))

    val dfWithDomainName = validUrl
      .select(
        $"hour",
        $"transactionuplinkbytes",
        $"transactiondownlinkbytes",
        $"httpurl",
        $"min",
        $"type"
      )
      .map(eachRow => {
        val http_url = eachRow.getString(3).split("/")
        (
          eachRow.getInt(0),
          eachRow.getLong(1),
          eachRow.getLong(2),
          eachRow.getInt(4),
          http_url(2),
          eachRow.getString(5)
        )
      })
      .toDF(
        "hour",
        "transactionuplinkbytes",
        "transactiondownlinkbytes",
        "min",
        "domainname",
        "type"
      )

    val tonnagePerType = dfWithDomainName
      .groupBy("hour", "min", "domainname", "type")
      .agg(
        (sum("transactionuplinkbytes") + sum("transactiondownlinkbytes"))
          .as("tonnagePerType")
      )
    val tonnagePerDomain = dfWithDomainName
      .groupBy("hour", "min", "domainname")
      .agg(
        (sum("transactionuplinkbytes") + sum("transactiondownlinkbytes"))
          .as("tonnagePerDomian")
      )
    val joinedDF =
      tonnagePerType.join(tonnagePerDomain, Seq("hour", "min", "domainname"))
    joinedDF
      .withColumn("percenatge", expr("tonnagePerType/tonnagePerDomian*100"))
      .select(
        "hour",
        "min",
        "domainname",
        "type",
        "percenatge",
        "tonnagePerDomian"
      )
      .write
      .partitionBy("hour", "min")
      .format("orc")
      .saveAsTable("ashwatth.question4")
  }
  def checkUrl(str: String): Boolean = {
    str.split("/").size >= 3
  }
}

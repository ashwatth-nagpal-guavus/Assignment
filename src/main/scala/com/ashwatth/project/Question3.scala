package com.ashwatth.project

import java.beans.Expression

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
object Question3 {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .appName("TopSubscribers")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._

      val edrHttpLogsDF = spark.table("ashwatth.edr_http_logs")
      //      .orc("../../ashwatth/minhour/")

      val validUrl =
        edrHttpLogsDF
          .filter($"httpUrl".isNotNull)
          .filter(eachRow => checkUrl(eachRow.getString(11)))

      val dfWithDomainName = validUrl
        .select(
          $"Hour",
          $"transactionUplinkBytes",
          $"transactionDownlinkBytes",
          $"httpUrl",
          $"min"
        )
        .map(eachRow => {
          val http_url = eachRow.getString(3).split("/")
          (
            eachRow.getInt(0),
            eachRow.getInt(1),
            eachRow.getLong(2),
            eachRow.getInt(4),
            http_url(2)
          )
        })
        .toDF(
          "Hour",
          "transactionUplinkBytes",
          "transactionDownlinkBytes",
          "min",
          "domainName"
        )

      val tonnageAndHitsPerMinute =
        dfWithDomainName
          .groupBy("Hour", "min", "domainName")
          .agg(
            (sum("transactionDownlinkBytes") + sum("transactionUplinkBytes"))
              .as("Tonnage"),
            count("domainName").as("Hits")
          )
          .orderBy(asc("Hour"), asc("min"))
      tonnageAndHitsPerMinute.write
        .partitionBy("Hour", "min")
        .format("orc")
        .saveAsTable("ashwatth.question3_ashwatth")
    } catch {
      case ex: Exception => println(ex)
    }

  }

  def checkUrl(str: String): Boolean = {
    str.split("/").size >= 3
  }

}

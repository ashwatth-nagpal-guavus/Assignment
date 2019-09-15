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
          .filter($"httpurl".isNotNull && $"httpreplycode".rlike("2[0-9][0-9]"))
          .filter(eachRow => checkUrl(eachRow.getString(11)))

      val dfWithDomainName = validUrl
        .select(
          $"hour",
          $"transactionuplinkbytes",
          $"transactiondownlinkbytes",
          $"httpurl",
          $"min"
        )
        .map(eachRow => {
          val http_url = eachRow.getString(3).split("/")
          (
            eachRow.getInt(0),
            eachRow.getLong(1),
            eachRow.getLong(2),
            eachRow.getInt(4),
            http_url(2)
          )
        })
        .toDF(
          "hour",
          "transactionuplinkbytes",
          "transactiondownlinkbytes",
          "min",
          "domainname"
        )

      val tonnageAndHitsPerMinute =
        dfWithDomainName
          .groupBy("hour", "min", "domainname")
          .agg(
            (sum("transactiondownlinkbytes") + sum("transactionuplinkbytes"))
              .as("Tonnage"),
            count("domainname").as("Hits")
          )
          .orderBy(asc("hour"), asc("min"))
      tonnageAndHitsPerMinute.write
        .partitionBy("hour", "min")
        .format("orc")
        .saveAsTable("ashwatth.question3")
    } catch {
      case ex: Exception => println(ex)
    }

  }

  def checkUrl(str: String): Boolean = {
    str.split("/").size >= 3
  }

}

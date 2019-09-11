package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Question3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TopSubscribers")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val smallSet = spark.read.orc("../../ashwatth/minhour/")

    val validUrl =
      smallSet
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
        .groupBy("min", "domainName")
        .agg(
          (sum("transactionDownlinkBytes") + sum("transactionUplinkBytes"))
            .as("Tonnage"),
          count("domainName").as("Hits")
        )
        .orderBy(asc("min"))
    tonnageAndHitsPerMinute.limit(20)

  }

  def checkUrl(str: String): Boolean = {
    str.split("/").size >= 3
  }

}

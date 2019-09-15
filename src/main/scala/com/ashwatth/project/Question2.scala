package com.ashwatth.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.Window

object Question2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TopSubscribers")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val smallSet = spark.table("ashwatth.edr_http_logs")
    val contentTypeDF =
      spark.read.csv("/tmp/ashwatth/tempMap").toDF("httpcontenttype", "type")
    val joinedDf = smallSet.join(contentTypeDF, Seq("httpcontenttype"))
    val aggDF = joinedDf
      .select(
        "radiususername",
        "transactionuplinkbytes",
        "transactiondownlinkbytes",
        "hour",
        "type"
      )
      .groupBy("hour", "radiususername", "type")
      .agg(
        sum("transactionuplinkbytes").as("upload"),
        sum("transactiondownlinkbytes").as("download"),
        count("type").as("hits")
      )

    val w1 = Window.partitionBy("radiususername").orderBy(desc("hits"))
    val top5TypeByHits = aggDF
      .select("radiususername", "hits", "type")
      .withColumn("rn", row_number.over(w1))
      .where($"rn" <= 5)
      .orderBy($"radiususername", asc("rn"))

    val w2 = Window.partitionBy("radiususername").orderBy(desc("upload"))
    val top5TypeByUpload = aggDF
      .select("radiususername", "upload", "type")
      .withColumn("rn", row_number.over(w2))
      .where($"rn" <= 5)
      .orderBy($"radiususername", asc("rn"))

    val w3 = Window.partitionBy("radiususername").orderBy(desc("download"))
    val top5TypeByDownload = aggDF
      .select("radiususername", "download", "type")
      .withColumn("rn", row_number.over(w3))
      .where($"rn" <= 5)
      .orderBy($"radiususername", asc("rn"))

    top5TypeByHits.write.format("orc").saveAsTable("ashwatth.question2_hits")
    top5TypeByUpload.write
      .format("orc")
      .saveAsTable("ashwatth.question2_upload")
    top5TypeByDownload.write
      .format("orc")
      .saveAsTable("ashwatth.question2_download")

    top5TypeByDownload
      .union(top5TypeByHits)
      .union(top5TypeByUpload)
      .write
      .format("orc")
      .saveAsTable("ashwatth.question2_unio_of_all")
  }
}

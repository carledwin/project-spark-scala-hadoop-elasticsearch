package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.spark.sql.{DataFrame, SparkSession}

object ClientReadParquetCpfHadoop {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.appName("ClientWriteReadParquet").config("spark.master", "local").getOrCreate()
    val pathParquetFile: String = "hdfs://cloudera@192.168.56.102/user/cloudera"
    val cpfsDF = parquetReader(spark, pathParquetFile)
    spark.close
  }

  def parquetReader(spark: SparkSession, pathParquetFile: String): DataFrame = {
    val cpfsDF = spark.read.parquet(pathParquetFile + "/parquet/cpfs1.parquet").toDF()
    cpfsDF.show
    //cpfsDF.write.parquet("./src/main/resources/download")
    cpfsDF
  }
}
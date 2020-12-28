package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.spark.sql.SparkSession

object ClientWriteParquetHadoop {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    val motoDF = spark.read.parquet("./src/main/resources/parquet/moto.parquet")
    motoDF.write.parquet("hdfs://cloudera@192.168.56.102:8022/user/cloudera/parquet/moto.parquet")
    motoDF.show
    spark.close
  }
}

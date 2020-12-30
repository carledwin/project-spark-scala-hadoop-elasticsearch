package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.spark.sql.SparkSession

object ClientWriteParquetCpfLocal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    import spark.implicits._
    val cpfsDF = Seq(("992.588.360-14"), ("490.232.420-25"), ("883.789.790-16")).toDF("cpf")
    cpfsDF.write.parquet("./src/main/resources/parquet/cpfs.parquet")
    cpfsDF.show
    spark.close
  }
}

package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.spark.sql.SparkSession

object ClientWriteParquetLocal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    import spark.implicits._
    val motoDF = Seq(("Africa Twin", "Honda", 54900), ("Himalayan", "Royal Enfield", 19390)).toDF("modelo","marca","valor")
    motoDF.write.parquet("./src/main/resources/parquet/moto2.parquet")
    motoDF.show
    spark.close
  }
}

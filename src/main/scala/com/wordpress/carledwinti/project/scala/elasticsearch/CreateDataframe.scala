package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.spark.sql.SparkSession

object CreateDataframe {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").getOrCreate

    import spark.implicits._
    val cpfDF = Seq(("132334"), ("65656767"), ("888999")).toDF("cpf")

    //cpfDF.foreach(cpf => println(cpf.get(0)))


  }
}

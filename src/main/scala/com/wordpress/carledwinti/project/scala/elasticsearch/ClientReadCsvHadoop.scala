package com.wordpress.carledwinti.project.scala.elasticsearch
import org.apache.spark.sql.SparkSession

object ClientReadCsvHadoop{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //val pathResource = "file:///C:/Users/carli/IdeaProjects/project-scala-elasticsearch/src/main/resources"
    val pathHdfs = "hdfs://cloudera@192.168.56.102:8022/user/cloudera"
    val sparkSession = SparkSession.builder.master("local[*]").getOrCreate()
    val carroDF = sparkSession.read.option("header", "true")csv(pathHdfs +"/csv/carro.csv")
    carroDF.show
    val carro2DF = sparkSession.read.option("header", "true")csv(pathHdfs +"/csv/carro2.csv")
    carro2DF.show
    sparkSession.close
  }
}
package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.http.HttpHost
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory, XContentType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ClientReadParquetCpfHadoopSaveElastic {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.appName("ClientWriteReadParquet").config("spark.master", "local").getOrCreate()
    val pathParquetFile: String = "hdfs://cloudera@192.168.56.102/user/cloudera"
    val cpfsDF = parquetReader(spark, pathParquetFile)

    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss")
    val now = LocalDateTime.now()
    val indexName = "cpfs" + now.format(dtf)
    val createIndexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    createIndexRequest.settings(Settings
      .builder
      .put("index.number_of_shards", 3)
      .put("index.number_of_replicas", 2))

    val alias: Alias = new Alias("cpfs_alias")
    createIndexRequest.alias(alias)
    createIndexRequest.mapping(createPropertiesWithXContentBuilder)

    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
    val restHighLevelClient: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)

    val createIndextResponse: CreateIndexResponse = restHighLevelClient.indices.create(createIndexRequest, RequestOptions.DEFAULT)
    println("result " + createIndextResponse.isAcknowledged)
    println("index " + createIndextResponse.index)

    val indexRequest: IndexRequest = new IndexRequest(createIndextResponse.index)
    indexRequest.source("{\"cpf\": \"4243566677777\"}", XContentType.JSON)

    val indexResponse: IndexResponse = restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT)
    println("index status " + indexResponse.status)
    restHighLevelClient.close
    spark.close
  }

  def createPropertiesWithXContentBuilder: XContentBuilder = {
    val xContentBuilder: XContentBuilder = XContentFactory.jsonBuilder
    xContentBuilder.startObject
    xContentBuilder.startObject("properties")
    xContentBuilder.startObject("cpf").field("type", "text")
    xContentBuilder.endObject
    xContentBuilder.endObject
    xContentBuilder.endObject
    xContentBuilder
  }

  def createCpfWithXContentBuilder: XContentBuilder = {
    val xContentBuilder: XContentBuilder = XContentFactory.jsonBuilder
    xContentBuilder.startObject
    xContentBuilder.startObject("index").field("")
    xContentBuilder.startObject("cpf").field("33333333333333")
    xContentBuilder.endObject
    xContentBuilder.endObject
    xContentBuilder.endObject
    xContentBuilder
  }

  def parquetReader(spark: SparkSession, pathParquetFile: String): DataFrame = {
    val cpfsDF = spark.read.parquet(pathParquetFile + "/parquet/cpfs1.parquet").toDF()
    cpfsDF.show
    cpfsDF
  }
}
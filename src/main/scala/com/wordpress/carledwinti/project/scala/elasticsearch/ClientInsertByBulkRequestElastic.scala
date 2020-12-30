package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.http.HttpHost
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory, XContentType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ClientInsertByBulkRequestElastic {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder.appName("ClientWriteReadParquet").config("spark.master", "local").getOrCreate()
    val pathParquetFile: String = "hdfs://cloudera@192.168.56.102/user/cloudera"
    val cpfsDF = parquetReader(spark, pathParquetFile).toDF("cpf")
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
    val restHighLevelClient: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    val createIndextResponse: CreateIndexResponse = executeCreateIndex(restHighLevelClient, createIndexRequest("cpf", createPropertiesWithXContentBuilder))
    val cpfList: List[Any] = convertCpfDFToList(cpfsDF)
    val bulkRequest: BulkRequest = new BulkRequest()

    addAllCpfsOnBullRequest(createIndextResponse.index, cpfList, bulkRequest)

    val bulkResponse: BulkResponse = restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT)
    println("bulk status " + bulkResponse.status)

    restHighLevelClient.close
    spark.close
  }

  def addAllCpfsOnBullRequest(indexName: String, cpfList: List[Any], bulkRequest: BulkRequest): Unit ={
    cpfList.toStream.foreach(c => {
      println("------------------->  :" + c)
      bulkRequest.add(new IndexRequest(indexName).source(XContentType.JSON, "cpf", c.toString))
    })
  }

  def convertCpfDFToList(cpfsDF: DataFrame): List[Any] = {
    cpfsDF.select("cpf").rdd.map(r =>  r(0)).collect().toList
  }

  def executeCreateIndex(restHighLevelClient: RestHighLevelClient, createIndexRequest: CreateIndexRequest): CreateIndexResponse = {
    val createIndextResponse: CreateIndexResponse = restHighLevelClient.indices.create(createIndexRequest, RequestOptions.DEFAULT)
    println("result " + createIndextResponse.isAcknowledged)
    println("index " + createIndextResponse.index)
    createIndextResponse
  }

  def createIndexRequest(name: String, xContentBuilder: XContentBuilder): CreateIndexRequest = {

    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss")
    val now = LocalDateTime.now()
    val indexName = name + now.format(dtf)

    val createIndexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    createIndexRequest.settings(Settings
      .builder
      .put("index.number_of_shards", 3)
      .put("index.number_of_replicas", 2))

    val alias: Alias = new Alias("cpfs_alias")
    createIndexRequest.alias(alias)
    createIndexRequest.mapping(xContentBuilder)

    createIndexRequest
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

  def parquetReader(spark: SparkSession, pathParquetFile: String): DataFrame = {
    val cpfsDF = spark.read.parquet(pathParquetFile + "/parquet/cpfs1.parquet").toDF()
    cpfsDF.show
    cpfsDF
  }
}
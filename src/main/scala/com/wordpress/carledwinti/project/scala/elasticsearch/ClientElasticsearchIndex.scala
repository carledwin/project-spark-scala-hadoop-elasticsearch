package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.http.HttpHost
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory, XContentType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Map
import java.util.HashMap
object ClientElasticsearchIndex {


  def main(args: Array[String]): Unit = {

    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss")
    val now = LocalDateTime.now()
    val indexName = "lojaindex" + now.format(dtf)
    val createIndexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    createIndexRequest.settings(Settings
                                .builder
                                .put("index.number_of_shards", 3)
                                .put("index.number_of_replicas", 2))

    val alias: Alias = new Alias("loja_alias")
    createIndexRequest.alias(alias)
    //createIndexRequest.mapping(createPropertiesWithMap)
    //createIndexRequest.mapping(createPropertiesWithXContentBuilder)
    //createIndexRequest.mapping(createIndexPropertiesStringJson, XContentType.JSON)
    //createIndexRequest.source(createIndexSourceStringJson, XContentType.JSON)


    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
    val restHighLevelClient: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)

    val createIndextResponse: CreateIndexResponse = restHighLevelClient.indices.create(createIndexRequest, RequestOptions.DEFAULT)
    println("result " + createIndextResponse.isAcknowledged)
    println("index " + createIndextResponse.index)
    restHighLevelClient.close
  }

  def createIndexPropertiesStringJson: String ={
    val stringJson: String = "{" +
      "\"properties\":{" +
      "\"modelo\":{\"type\":\"text\"}," +
      "\"marca\":{\"type\":\"text\"}," +
      "\"anof\":{\"type\":\"integer\"}," +
      "\"anom\":{\"type\":\"integer\"}," +
      "\"valor\":{\"type\":\"double\"}" +
      "}" +
      "}"
    stringJson
  }

  def createIndexSourceStringJson: String ={
    val stringJson: String = "{" +
      "\"settings\":{" +
      "\"number_of_shards\":3," +
      "\"number_of_replicas\":2" +
      "}," +
      "\"mappings\":{" +
      "\"properties\":{" +
      "\"modelo\":{\"type\":\"text\"}," +
      "\"marca\":{\"type\":\"text\"}," +
      "\"anof\":{\"type\":\"integer\"}," +
      "\"anom\":{\"type\":\"integer\"}," +
      "\"valor\":{\"type\":\"double\"}" +
      "}" +
      "}" +
      "}"
    stringJson
  }

  def createPropertiesWithXContentBuilder: XContentBuilder = {
    val xContentBuilder: XContentBuilder = XContentFactory.jsonBuilder
    xContentBuilder.startObject
      xContentBuilder.startObject("properties")
        xContentBuilder.startObject("modelo").field("type", "text")
        xContentBuilder.endObject
        xContentBuilder.startObject("marca").field("type", "text")
        xContentBuilder.endObject
        xContentBuilder.startObject("anof").field("type", "integer")
        xContentBuilder.endObject
        xContentBuilder.startObject("anom").field("type", "integer")
        xContentBuilder.endObject
        xContentBuilder.startObject("valor").field("type", "double")
        xContentBuilder.endObject
      xContentBuilder.endObject
    xContentBuilder.endObject
    xContentBuilder
  }
  def createPropertiesWithMap: Map[String, Object] = {

    val modelo:Map[String, String] = new HashMap[String, String]
    val marca:Map[String, String] = new HashMap[String, String]
    val anof:Map[String, String] = new HashMap[String, String]
    val anom:Map[String, String] = new HashMap[String, String]
    val valor:Map[String, String] = new HashMap[String, String]

    modelo.put("type", "text")
    marca.put("type", "text")
    anof.put("type", "integer")
    anom.put("type", "integer")
    valor.put("type", "double")

    val properties:Map[String, Object] = new HashMap[String, Object]
    properties.put("modelo", modelo)
    properties.put("marca", marca)
    properties.put("anof", anof)
    properties.put("anom", anom)
    properties.put("valor", valor)

    val mapping:Map[String, Object] = new HashMap[String, Object]
    mapping.put("properties", properties)
    mapping
  }
}

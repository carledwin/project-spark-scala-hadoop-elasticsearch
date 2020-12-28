package com.wordpress.carledwinti.project.scala.elasticsearch

import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHost, RequestLine, StatusLine}
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.{Request, RequestOptions, Response, RestClient}
import org.elasticsearch.common.settings.Settings

import java.util

object RestClientElasticSearchApp {

  def main(args: Array[String]): Unit = {

    println("Iniciando o client ElasticSearch com Scala")

    case class Carro(nome: String, marca: String)

    val restClient: RestClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build()

    val request: Request = new Request("GET", "/carro/_search")
    val response: Response = restClient.performRequest(request)
    val requestLine: RequestLine = response.getRequestLine
    val httpHost :HttpHost = response.getHost
    val statusLine:StatusLine = response.getStatusLine
    val responseBody: String = EntityUtils.toString(response.getEntity)
    println(responseBody)

    println("Finalizado")
    restClient.close()


  }

}

package cn.pandadb.batchimport


import java.util
import java.io.FileWriter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.neo4j.graphdb._
import org.neo4j.values.storable.Values
import org.neo4j.values.storable._
import org.apache.http.HttpHost
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory, XContentType}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.common.Strings
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.mustache.{SearchTemplateRequest, SearchTemplateResponse}



class ImportNodeToEs(host: String, port: Int, schema: String, indexName: String, typeName: String) {


  val idFieldName = "id" // 存id字段名称
  val labelFieldName = "labels" // 存label字段名称
  val propFieldNamePrefix = "" // 属性字段名称前缀
  val countPerCommit = 1000 // 一次批量写入间隔节点数
  val countPerLog = 10000 // 日志一次写入间隔节点数

  private def createClient(host: String, port: Int, schema: String = "http"): RestHighLevelClient = {
    val httpHost = new HttpHost(host, port, schema)
    val builder = RestClient.builder(httpHost)
    val client = new RestHighLevelClient(builder)
    if (!indexExists(client, indexName)){
      System.out.println("create index: "+indexName)
      val res = createIndex(client, indexName,typeName)
      if (res) System.out.println("index create success!")
      else System.out.println("index create failed!")
    }
    client
  }

  private def indexExists(client: RestHighLevelClient, indexName: String): Boolean = {
    val request = new GetIndexRequest()
    request.indices(indexName)
    client.indices.exists(request, RequestOptions.DEFAULT)
  }

  private def createIndex(client: RestHighLevelClient, indexName: String, typeName: String): Boolean = {
    val indexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    indexRequest.mapping(typeName,"{\"_all\":{\"type\":\"text\"}}", XContentType.JSON)
    val indexResponse: CreateIndexResponse = client.indices().create(indexRequest, RequestOptions.DEFAULT)
    indexResponse.isAcknowledged
  }


  private def addData(client: RestHighLevelClient, indexName: String, typeName: String, id: String, builder: XContentBuilder): String = {
    val indexRequest: IndexRequest = new IndexRequest(indexName, typeName, id)
    indexRequest.source(builder)
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
    val indexResponse: IndexResponse = client.index(indexRequest)
    indexResponse.getId
  }


  def doImport(nodes: ResourceIterator[Node], logFileWriter: FileWriter): Unit = {
    val esClient = createClient(host, port, schema)

    var nodeCount = 0
    var bulkRequest = new BulkRequest()
//    bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)

    System.out.println(s"$nodeCount,${System.currentTimeMillis}\n")
    logFileWriter.write(s"$nodeCount,${System.currentTimeMillis}\n")

    while (nodes.hasNext) {
      nodeCount += 1
      val n1 = nodes.next
      val indexId = n1.getId.toString
      var indexRequest = new IndexRequest(indexName, typeName, indexId)

      val builder = XContentFactory.jsonBuilder
      builder.startObject()
      builder.field(idFieldName, n1.getId)

      val labels = n1.getLabels.asScala
      builder.startArray(labelFieldName)
      for (lbl <- labels) {
        builder.value(lbl.name())
      }
      builder.endArray()

      val props = n1.getAllProperties

      for (entry <- props.entrySet.asScala) {
        val k = propFieldNamePrefix + entry.getKey
        val v = entry.getValue
        if (! v.getClass.isArray){
          builder.field(k, v)
        }
        else {
          builder.startArray(k)

          if (v.isInstanceOf[Array[Boolean]]){
            v.asInstanceOf[Array[Boolean]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Int]]){
            v.asInstanceOf[Array[Int]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Long]]){
            v.asInstanceOf[Array[Long]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Short]]){
            v.asInstanceOf[Array[Short]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Float]]){
            v.asInstanceOf[Array[Float]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Double]]){
            v.asInstanceOf[Array[Double]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[Char]]){
            v.asInstanceOf[Array[Char]].foreach(v1=>builder.value(v1))
          }
          if (v.isInstanceOf[Array[String]]){
            v.asInstanceOf[Array[String]].foreach(v1=>builder.value(v1))
          }

          builder.endArray()
        }
      }

      builder.endObject()

      indexRequest.source(builder)

      bulkRequest.add(indexRequest)

      if (nodeCount % countPerCommit == 0) {
        esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
        bulkRequest = new BulkRequest()
//        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
      }
      if (nodeCount % countPerLog == 0) {
        System.out.println(s"$nodeCount,${System.currentTimeMillis}\n")
        logFileWriter.write(s"$nodeCount,${System.currentTimeMillis}\n")
        logFileWriter.flush()
      }
    }
    if (nodeCount % countPerCommit != 0) {
      esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    }

    esClient.close()

    if (nodeCount % countPerLog != 0) {
      System.out.println(s"$nodeCount,${System.currentTimeMillis}\n")
      logFileWriter.write(s"$nodeCount,${System.currentTimeMillis}\n")
      logFileWriter.flush()
    }
  }
}

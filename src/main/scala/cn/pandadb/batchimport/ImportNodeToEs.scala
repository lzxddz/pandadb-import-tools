package cn.pandadb.batchimport


import java.util
import java.io.FileWriter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.neo4j.graphdb._
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

  var esClient = createClient(host, port, schema)

  val idFieldName = "id" // 存id字段名称
  val labelFieldName = "labels" // 存label字段名称
  val propFieldNamePrefix = "" // 属性字段名称前缀
  val countPerCommit = 1000 // 一次批量写入间隔节点数
  val countPerLog = 10000 // 日志一次写入间隔节点数

  def createClient(host: String, port: Int, schema: String = "http"): RestHighLevelClient = {
    val httpHost = new HttpHost(host, port, schema)
    val builder = RestClient.builder(httpHost)
    val client = new RestHighLevelClient(builder)
    if (!indexExists(client, indexName)){
      createIndex(client, indexName,typeName)
    }
    client
  }

  def indexExists(client: RestHighLevelClient, indexName: String): Boolean = {
    val request = new GetIndexRequest()
    request.indices(indexName)
    client.indices.exists(request, RequestOptions.DEFAULT)
  }

  def createIndex(client: RestHighLevelClient, indexName: String, typeName: String): Boolean = {
    val indexRequest: CreateIndexRequest = new CreateIndexRequest(indexName)
    indexRequest.mapping(typeName,"{\"_all\":{\"type\":\"text\"}}", XContentType.JSON)
    val indexResponse: CreateIndexResponse = client.indices().create(indexRequest, RequestOptions.DEFAULT)
    indexResponse.isAcknowledged
  }


  def addData(client: RestHighLevelClient, indexName: String, typeName: String, id: String, builder: XContentBuilder): String = {
    val indexRequest: IndexRequest = new IndexRequest(indexName, typeName, id)
    indexRequest.source(builder)
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
    val indexResponse: IndexResponse = client.index(indexRequest)
    indexResponse.getId
  }

  def doImport(nodes: ResourceIterator[Node], logFileWriter: FileWriter): Unit = {
    var nodeCount = 0
    var bulkRequest = new BulkRequest()
    while (nodes.hasNext) {
      val n1 = nodes.next
      val indexId = n1.getId.toString
      val indexRequest = new IndexRequest(indexName, typeName, indexId)

      val builder = XContentFactory.jsonBuilder
      builder.startObject()
      builder.field(idFieldName, n1.getId)

      val labels = n1.getLabels.asScala
      builder.startArray(labelFieldName)
      for (lbl <- labels) {
        builder.field(lbl.name)
      }
      builder.endArray()

      val props = n1.getAllProperties

      for (entry <- props.entrySet.asScala) {
        builder.field(propFieldNamePrefix + entry.getKey, entry.getValue)
      }

      indexRequest.source(builder)
      bulkRequest.add(indexRequest)
      nodeCount += 1
      if (nodeCount % countPerCommit == 0) {
        esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
        bulkRequest = new BulkRequest()
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

    if (nodeCount % countPerLog != 0) {
      System.out.println(s"$nodeCount,${System.currentTimeMillis}\n")
      logFileWriter.write(s"$nodeCount,${System.currentTimeMillis}\n")
      logFileWriter.flush()
    }
  }
}

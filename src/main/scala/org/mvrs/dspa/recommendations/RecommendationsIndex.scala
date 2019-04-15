package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, longField, nestedField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.{ElasticSearchNode, ElasticSearchIndexSink}

import scala.collection.JavaConverters._

class RecommendationsIndex(hosts: Seq[ElasticSearchNode], indexName: String, typeName: String)
  extends ElasticSearchIndexSink[(Long, Seq[(Long, Double)])](hosts, indexName, typeName) {
  override protected def createDocument(record: (Long, Seq[(Long, Double)])): Map[String, Any] = Map[String, Any](
    "users" -> record._2.map(createNestedDocument).toList.asJava,
    "lastUpdate" -> System.currentTimeMillis())

  private def createNestedDocument(t: (Long, Double)) = Map[String, Any](
    "uid" -> t._1,
    "similarity" -> t._2)
    .asJava

  override protected def getId(record: (Long, Seq[(Long, Double)])): String = record._1.toString

  override protected def createFields(): Iterable[FieldDefinition] = Seq(
    nestedField("users").fields(
      longField("uid").index(false),
      doubleField("similarity").index(false)
    ),
    dateField("lastUpdate"))
}

//object RecommendationsIndex {
//  def createSink(hostname: String, port: Int, scheme: String,
//                 indexName: String, typeName: String): ElasticsearchSink[(Long, Seq[(Long, Double)])] = {
//    val hosts = List(new HttpHost(hostname, port, scheme))
//
//    val esSinkBuilder = new ElasticsearchSink.Builder[(Long, Seq[(Long, Double)])](
//      hosts.asJava,
//      new ElasticsearchSinkFunction[(Long, Seq[(Long, Double)])] {
//        def createUpsertRequest(record: (Long, Seq[(Long, Double)])): UpdateRequest = {
//
//          val document = createDocument(record)
//
//          val id = record._1.toString
//          val indexRequest = new IndexRequest(indexName, typeName, id).source(document)
//          new UpdateRequest(indexName, typeName, id).doc(document).upsert(indexRequest)
//        }
//
//        override def process(record: (Long, Seq[(Long, Double)]), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//          requestIndexer.add(createUpsertRequest(record))
//        }
//      }
//    )
//
//    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
//    esSinkBuilder.setBulkFlushMaxActions(100)
//
//    //    // provide a RestClientFactory for custom configuration on the internally created REST client
//    //    //    esSinkBuilder.setRestClientFactory(
//    //    //      restClientBuilder => {
//    //    //         restClientBuilder.setDefaultHeaders(...)
//    //    //         restClientBuilder.setMaxRetryTimeoutMillis(...)
//    //    //         restClientBuilder.setPathPrefix(...)
//    //    //         restClientBuilder.setHttpClientConfigCallback(...)
//    //    //      }
//    //    //    )
//    //
//    esSinkBuilder.build()
//  }
//
//  private def createDocument(record: (Long, Seq[(Long, Double)])): util.Map[String, Any] = {
//    Map[String, Any](
//      "users" -> record._2.map(createNestedDocument).toList.asJava,
//      "lastUpdate" -> System.currentTimeMillis())
//      .asJava
//  }
//
//  private def createNestedDocument(t: (Long, Double)) = {
//    Map[String, Any](
//      "uid" -> t._1,
//      "similarity" -> t._2)
//      .asJava
//  }
//
//  def create(client: ElasticClient, indexName: String, typeName: String): Unit = {
//    import com.sksamuel.elastic4s.http.ElasticDsl._
//
//    client.execute {
//      createIndex(indexName).mappings(mapping(typeName).fields(createFields()))
//    }.await
//  }
//
//  private def createFields(): Iterable[FieldDefinition] = Seq(
//    nestedField("users").fields(
//      longField("uid").index(false),
//      doubleField("similarity").index(false)
//    ),
//    dateField("lastUpdate"))
//}

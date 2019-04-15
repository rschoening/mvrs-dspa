package org.mvrs.dspa.utils

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

abstract class ElasticSearchIndexSink[T](hosts: Seq[ElasticSearchNode], indexName: String, typeName: String)
  extends ElasticSearchIndex(hosts, indexName, typeName) {
  
  def createSink(numMaxActions: Int): ElasticsearchSink[T] = {
    val builder = new ElasticsearchSink.Builder[T](
      hosts.map(_.httpHost).asJava,
      new ElasticsearchSinkFunction[T] {
        private def createUpsertRequest(record: T): UpdateRequest = {

          val document = createDocument(record).asJava
          val id = getId(record)

          val indexRequest = new IndexRequest(indexName, typeName, id).source(document)
          new UpdateRequest(indexName, typeName, id).doc(document).upsert(indexRequest)
        }

        override def process(record: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createUpsertRequest(record))
        }
      }
    )

    // configuration for the bulk requests
    builder.setBulkFlushMaxActions(numMaxActions)

    //    // provide a RestClientFactory for custom configuration on the internally created REST client
    //    //    esSinkBuilder.setRestClientFactory(
    //    //      restClientBuilder => {
    //    //         restClientBuilder.setDefaultHeaders(...)
    //    //         restClientBuilder.setMaxRetryTimeoutMillis(...)
    //    //         restClientBuilder.setPathPrefix(...)
    //    //         restClientBuilder.setHttpClientConfigCallback(...)
    //    //      }
    //    //    )
    //

    builder.build()
  }

  protected def getId(record: T): String

  protected def createDocument(record: T): Map[String, Any]
}

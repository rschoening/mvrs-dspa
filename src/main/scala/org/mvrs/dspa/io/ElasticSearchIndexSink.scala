package org.mvrs.dspa.io

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

abstract class ElasticSearchIndexSink[T](indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndex(indexName, typeName, nodes: _*) {

  def createSink(batchSize: Int): ElasticsearchSink[T] = {
    require(batchSize > 0, s"invalid batch size: $batchSize")

    val builder = new ElasticsearchSink.Builder[T](
      nodes.map(_.httpHost).asJava,
      new ElasticsearchSinkFunction[T] {
        private def createUpsertRequest(record: T): UpdateRequest = {

          // NOTE this closes over the containing class, which therefore must be serializable
          val document = createDocument(record).asJava
          val id = getDocumentId(record)

          val indexRequest = new IndexRequest(indexName, typeName, id).source(document)
          new UpdateRequest(indexName, typeName, id).doc(document).upsert(indexRequest)
        }

        override def process(record: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createUpsertRequest(record))
        }
      }
    )

    // configuration for the bulk requests
    builder.setBulkFlushMaxActions(batchSize)

    // if ever needed: provide a RestClientFactory for custom configuration on the internally created REST client
    //
    //    builder.setRestClientFactory(
    //      restClientBuilder => {
    //         restClientBuilder.setDefaultHeaders(...)
    //         restClientBuilder.setMaxRetryTimeoutMillis(...)
    //         restClientBuilder.setPathPrefix(...)
    //         restClientBuilder.setHttpClientConfigCallback(...)
    //      }
    //    )

    builder.build()
  }

  protected def getDocumentId(record: T): String

  protected def createDocument(record: T): Map[String, Any]
}

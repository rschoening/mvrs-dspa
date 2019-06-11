package org.mvrs.dspa.utils.elastic

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

/**
  * Base class for ElasticSearch index gateway classes that provide a sink for use with the streaming API,
  * writing documents using upsert operations.
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  * @tparam T The input record type
  */
abstract class ElasticSearchIndexSink[T](indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndex(indexName, nodes: _*) {

  /**
    * Creates the streaming sink for the index
    *
    * @param batchSize         The batch size for the sink
    * @param bulkFlushInterval The optional bulk flush interval (in milliseconds)
    * @return The sink
    */
  def createSink(batchSize: Int, bulkFlushInterval: Option[Long] = None): ElasticsearchSink[T] = {
    require(batchSize > 0, s"invalid batch size: $batchSize")

    val builder = new ElasticsearchSink.Builder[T](
      nodes.map(_.httpHost).asJava,
      new ElasticsearchSinkFunction[T] {
        private def createUpsertRequest(record: T): UpdateRequest = {

          // NOTE this closes over the containing class, which therefore must be serializable
          val document = createDocument(record).asJava
          val id = getDocumentId(record)

          val indexRequest = new IndexRequest(indexName, id).source(document)
          new UpdateRequest(indexName, typeName, id).doc(document).upsert(indexRequest)
        }

        override def process(record: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createUpsertRequest(record))
        }
      }
    )

    // configuration for the bulk requests
    builder.setBulkFlushMaxActions(batchSize)
    bulkFlushInterval.foreach(builder.setBulkFlushInterval)

    builder.build()
  }

  protected def getDocumentId(record: T): String

  protected def createDocument(record: T): Map[String, Any]
}




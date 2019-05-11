package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http.ElasticDsl._

abstract class ElasticSearchIndexWithUpsertOutputFormat[T](indexName: String,
                                                           nodes: Seq[ElasticSearchNode],
                                                           batchSize: Int = 1000)
  extends ElasticSearchIndex(indexName, nodes: _*) {

  /**
    * Creates the output format performing upserts (based on the document id)
    *
    * @return the output format to be used with the Dataset API
    */
  def createUpsertFormat(): ElasticSearchOutputFormat[T] = {
    new ElasticSearchBulkOutputFormat[T](indexName, typeName, nodes, batchSize) {

      override protected def createRequest(record: T): BulkCompatibleRequest =
        update(getDocumentId(record)) in indexName / typeName docAsUpsert createDocument(record)
    }
  }

  protected def getDocumentId(record: T): String

  protected def createDocument(record: T): Map[String, Any]
}

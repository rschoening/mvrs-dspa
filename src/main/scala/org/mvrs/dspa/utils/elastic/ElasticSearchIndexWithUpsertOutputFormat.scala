package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.ElasticClient

abstract class ElasticSearchIndexWithUpsertOutputFormat[T](indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndex(indexName, nodes: _*) {

  def createUpsertFormat(): ElasticSearchOutputFormat[T] = {
    new ElasticSearchOutputFormat[T](nodes: _*) {

      import com.sksamuel.elastic4s.http.ElasticDsl._

      override def process(record: T, client: ElasticClient): Unit = {
        client.execute {
          update(getDocumentId(record)) in indexName / typeName docAsUpsert createDocument(record)
        }.await
      }
    }
  }

  protected def getDocumentId(record: T): String

  protected def createDocument(record: T): Map[String, Any]
}
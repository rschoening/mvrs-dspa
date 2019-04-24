package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.ElasticClient

abstract class ElasticSearchUpsertOutputFormat[T](indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchOutputFormat[T](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  // TODO add support for batching and/or capacity-bounded async calls
  override def process(record: T, client: ElasticClient): Unit = {
    client.execute {
      update(getDocumentId(record)) in indexName / typeName docAsUpsert createDocument(record)
    }.await
  }

  def getDocumentId(record: T): String

  def createDocument(record: T): Map[String, Any]
}

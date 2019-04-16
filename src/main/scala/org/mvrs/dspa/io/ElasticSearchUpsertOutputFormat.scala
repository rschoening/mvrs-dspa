package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.ElasticClient

abstract class ElasticSearchUpsertOutputFormat[T](indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchOutputFormat[T](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: T, client: ElasticClient): Unit = {
    client.execute {
      update(getId(record)) in indexName / typeName docAsUpsert getFields(record)
    }.await
  }

  def getId(record: T): String

  def getFields(record: T): Iterable[(String, Any)]
}

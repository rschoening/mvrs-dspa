package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.ElasticClient

abstract class ElasticSearchUpsertOutputFormat[T](uri: String, indexName: String, typeName: String)
  extends ElasticSearchOutputFormat[T](uri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: T, client: ElasticClient): Unit = {
    client.execute {
      update(getId(record)) in indexName / typeName docAsUpsert getFields(record)
    }.await
  }

  def getId(record: T): String

  def getFields(record: T): Iterable[(String, Any)]
}

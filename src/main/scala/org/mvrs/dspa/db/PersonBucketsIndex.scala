package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, longField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

class PersonBucketsIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[Long])](indexName, nodes) {

  override def getDocumentId(record: (Long, List[Long])): String = record._1.toString

  override def createDocument(record: (Long, List[Long])): Map[String, Any] =
    Map(
      "uid" -> record._2,
      "lastUpdate" -> System.currentTimeMillis()
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      longField("uid").index(false),
      dateField("lastUpdate")
    )
}
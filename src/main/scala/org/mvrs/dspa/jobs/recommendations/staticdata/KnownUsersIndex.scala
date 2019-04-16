package org.mvrs.dspa.jobs.recommendations.staticdata

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, longField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

class KnownUsersIndex(indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[Long])](indexName, typeName, nodes: _*) {

  override def getDocumentId(record: (Long, List[Long])): String = record._1.toString

  override def createDocument(record: (Long, List[Long])): Map[String, Any] =
    Map(
      "knownUsers" -> record._2,
      "lastUpdate" -> System.currentTimeMillis
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      longField("knownUsers").index(false),
      dateField("lastUpdate")
    )
}

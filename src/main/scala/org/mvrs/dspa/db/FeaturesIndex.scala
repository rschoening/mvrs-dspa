package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, textField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

class FeaturesIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[String])](indexName, nodes: _*) {
  override protected def getDocumentId(record: (Long, List[String])): String = record._1.toString

  override protected def createDocument(record: (Long, List[String])): Map[String, Any] =
    Map(
      "features" -> record._2,
      "lastUpdate" -> System.currentTimeMillis
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      textField("features").index(false),
      dateField("lastUpdate")
    )
}

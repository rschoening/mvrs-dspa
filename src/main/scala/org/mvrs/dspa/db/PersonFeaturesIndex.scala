package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, textField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

/**
  * Person features index (recommendations job)
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class PersonFeaturesIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[String])](indexName, nodes) {

  override protected def getDocumentId(record: (Long, List[String])): String = record._1.toString

  /**
    * Creates the document to insert
    *
    * @param record tuple of person id and list of feature ids
    * @return document map
    */
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

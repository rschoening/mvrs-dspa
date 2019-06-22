package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, keywordField, textField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

/**
  * Index of forum features (recommendations job)
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class ForumFeaturesIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, String, List[String])](indexName, nodes) {

  override protected def getDocumentId(record: (Long, String, List[String])): String = record._1.toString

  /**
    * Creates the document to insert
    *
    * @param record tuple of forum id, forum title and list of feature ids
    * @return document map
    */
  override protected def createDocument(record: (Long, String, List[String])): Map[String, Any] =
    Map(
      "title" -> record._2,
      "features" -> record._3,
      "lastUpdate" -> System.currentTimeMillis
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      keywordField("title"),
      textField("features").index(false),
      dateField("lastUpdate")
    )
}

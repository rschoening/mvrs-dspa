package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, longField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

/**
  * Index of persons already known to a person (recommendations job)
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class KnownUsersIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[Long])](indexName, nodes) {

  override def getDocumentId(record: (Long, List[Long])): String = record._1.toString // person id

  /**
    * Creates the document to insert
    *
    * @param record tuple of person id, list of person ids
    * @return document map
    */
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

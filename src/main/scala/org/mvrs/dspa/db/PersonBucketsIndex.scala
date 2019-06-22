package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, longField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

/**
  * Index of LSH buckets containing person ids
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class PersonBucketsIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, List[Long])](indexName, nodes) {

  override def getDocumentId(record: (Long, List[Long])): String = record._1.toString // bucket id

  /**
    * Creates the document to insert
    *
    * @param record tuple of bucket id and list of person ids
    * @return document map
    */
  override def createDocument(record: (Long, List[Long])): Map[String, Any] =
    Map(
      "uid" -> record._2,
      "lastUpdate" -> System.currentTimeMillis()
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      longField("uid").index(false), // list of person ids
      dateField("lastUpdate")
    )
}
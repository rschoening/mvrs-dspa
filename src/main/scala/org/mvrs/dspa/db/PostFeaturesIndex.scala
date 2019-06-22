package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.PostFeatures
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

/**
  * Index of post features (recommendations job)
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class PostFeaturesIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[PostFeatures](indexName, nodes: _*) {
  override protected def getDocumentId(record: PostFeatures): String = record.postId.toString

  /**
    * Creates the document to insert
    *
    * @param record post features record
    * @return document map
    */
  override protected def createDocument(record: PostFeatures): Map[String, Any] =
    Map(
      "personId" -> record.personId,
      "features" -> record.features.asJava,
      "timestamp" -> record.timestamp,
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Iterable(
      longField("personId"),
      textField("features").index(false),
      dateField("timestamp"),
    )
}

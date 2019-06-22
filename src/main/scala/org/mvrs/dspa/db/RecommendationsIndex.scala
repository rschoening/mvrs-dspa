package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, longField, nestedField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.{Recommendation, RecommendedPerson}
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

/**
  * Index of person recommendations per person
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class RecommendationsIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[(Recommendation, Long)](indexName, nodes: _*) {

  /**
    * Creates the document to insert
    *
    * @param record tuple of recommendation (top 5 most similar persons ordered by similarity) and timestamp
    * @return document as map
    */
  override protected def createDocument(record: (Recommendation, Long)): Map[String, Any] =
    Map(
      "users" -> record._1.recommendations.map(createNestedDocument).toList.asJava,
      "lastUpdate" -> record._2
    )

  override protected def getDocumentId(record: (Recommendation, Long)): String = record._1.personId.toString

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      nestedField("users").fields(
        longField("uid").index(false),
        doubleField("similarity").index(false)
      ),
      dateField("lastUpdate"))

  private def createNestedDocument(t: RecommendedPerson) =
    Map(
      "uid" -> t.personId,
      "similarity" -> t.similarity
    ).asJava
}




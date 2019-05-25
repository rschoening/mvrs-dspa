package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, longField, nestedField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.{Recommendation, RecommendedPerson}
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

class RecommendationsIndex(indexName: String, hosts: ElasticSearchNode*)
  extends ElasticSearchIndexSink[(Recommendation, Long)](indexName, hosts: _*) {

  override protected def createDocument(record: (Recommendation, Long)): Map[String, Any] = Map[String, Any](
    "users" -> record._1.recommendations.map(createNestedDocument).toList.asJava,
    "lastUpdate" -> record._2) // System.currentTimeMillis())

  override protected def getDocumentId(record: (Recommendation, Long)): String = record._1.personId.toString

  override protected def createFields(): Iterable[FieldDefinition] = Seq(
    nestedField("users").fields(
      longField("uid").index(false),
      doubleField("similarity").index(false)
    ),
    dateField("lastUpdate"))

  private def createNestedDocument(t: RecommendedPerson) =
    Map[String, Any](
      "uid" -> t.personId,
      "similarity" -> t.similarity
    ).asJava
}




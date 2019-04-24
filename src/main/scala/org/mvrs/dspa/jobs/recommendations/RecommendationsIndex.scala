package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, longField, nestedField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

class RecommendationsIndex(indexName: String, hosts: ElasticSearchNode*)
  extends ElasticSearchIndexSink[(Long, Seq[(Long, Double)])](indexName, hosts: _*) {

  override protected def createDocument(record: (Long, Seq[(Long, Double)])): Map[String, Any] = Map[String, Any](
    "users" -> record._2.map(createNestedDocument).toList.asJava,
    "lastUpdate" -> System.currentTimeMillis())

  override protected def getDocumentId(record: (Long, Seq[(Long, Double)])): String = record._1.toString

  override protected def createFields(): Iterable[FieldDefinition] = Seq(
    nestedField("users").fields(
      longField("uid").index(false),
      doubleField("similarity").index(false)
    ),
    dateField("lastUpdate"))

  private def createNestedDocument(t: (Long, Double)) =
    Map[String, Any](
      "uid" -> t._1,
      "similarity" -> t._2
    ).asJava
}


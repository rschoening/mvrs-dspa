package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, intField, keywordField, longField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.{ClassifiedEvent, EventType}
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

class ActivityClassificationIndex(indexName: String, esNode: ElasticSearchNode*)
  extends ElasticSearchIndexSink[ClassifiedEvent](indexName, esNode: _*) {

  override protected def getDocumentId(record: ClassifiedEvent): String = s"${record.eventId}#${getKeySuffix(record)}"

  override protected def createDocument(record: ClassifiedEvent): Map[String, Any] =
    Map(
      "personId" -> record.personId,
      "eventType" -> record.eventType.toString,
      "eventId" -> record.eventId,
      "clusterIndex" -> record.cluster.index,
      "clusterLabel" -> record.cluster.labelText,
      "clusterWeight" -> record.cluster.weight,
      "clusterCentroid" -> record.cluster.centroid.features.asJava, // Note: conversion to Java collection required
      "timestamp" -> record.timestamp
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Iterable(
      longField("personId"),
      keywordField(name = "eventType"),
      longField(name = "eventId"),
      intField(name = "clusterIndex"),
      keywordField(name = "clusterLabel"),
      doubleField(name = "clusterWeight"),
      doubleField(name = "clusterCentroid").index(false),
      dateField("timestamp")
    )

  private def getKeySuffix(record: ClassifiedEvent) = record.eventType match {
    case EventType.Comment => "c"
    case EventType.Like => "l"
    case EventType.Post => "p"
    case EventType.Reply => "r"
  }
}



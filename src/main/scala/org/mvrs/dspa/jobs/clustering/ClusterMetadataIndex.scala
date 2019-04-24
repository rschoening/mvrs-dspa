package org.mvrs.dspa.jobs.clustering

import java.util

import com.sksamuel.elastic4s.http.ElasticDsl.{dateField, doubleField, intField, keywordField, nestedField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexSink, ElasticSearchNode}
import org.mvrs.dspa.jobs.clustering.KMeansClusterFunction.ClusterMetadata

import scala.collection.JavaConverters._

class ClusterMetadataIndex(indexName: String, typeName: String, esNode: ElasticSearchNode*)
  extends ElasticSearchIndexSink[ClusterMetadata](indexName, typeName, esNode: _*) {

  override protected def getDocumentId(record: ClusterMetadata): String = s"${record.timestamp}"

  override protected def createDocument(record: ClusterMetadata): Map[String, Any] = Map[String, Any](
    "timestamp" -> record.timestamp,
    "k" -> record.clusters.size,
    "kDifference" -> record.kDifference,
    "averageDistance" -> record.averageVectorDistance,
    "averageWeightDifference" -> record.averageWeightDifference,
    "clusters" -> record.clusters.map(createNestedDocument).toList.asJava,
  )

  override protected def createFields(): Iterable[FieldDefinition] =
    Iterable(
      dateField("timestamp"),
      doubleField("k"),
      doubleField("kDifference"),
      doubleField(name = "averageDistance"),
      doubleField(name = "averageWeightDifference"),
      nestedField("clusters").fields(
        intField(name = "index"),
        keywordField(name = "label"),
        doubleField(name = "weight"),
        doubleField(name = "centroid").index(false),
        doubleField(name = "differences").index(false),
        doubleField(name = "differenceLength"),
        doubleField(name = "weightDifference")
      ),
    )

  private def createNestedDocument(t: (Cluster, scala.Vector[Double], Double, Double)): util.Map[String, Any] =
    Map[String, Any](
      "index" -> t._1.index,
      "label" -> t._1.labelText,
      "weight" -> t._1.weight,
      "centroid" -> t._1.centroid.features.asJava,
      "differences" -> t._2.asJava,
      "differenceLength" -> t._3,
      "weightDifference" -> t._4
    ).asJava
}

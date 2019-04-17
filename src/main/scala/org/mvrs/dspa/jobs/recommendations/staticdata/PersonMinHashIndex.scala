package org.mvrs.dspa.jobs.recommendations.staticdata

import java.util.Base64

import com.sksamuel.elastic4s.http.ElasticDsl.{binaryField, dateField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

class PersonMinHashIndex(indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, MinHashSignature)](indexName, typeName, nodes: _*) {

  override def getDocumentId(record: (Long, MinHashSignature)): String = record._1.toString

  override def createDocument(record: (Long, MinHashSignature)): Map[String, Any] =
    Map(
      "minhash" -> Base64.getEncoder.encodeToString(record._2.bytes),
      "lastUpdate" -> System.currentTimeMillis()
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      binaryField("minhash").index(false),
      dateField("lastUpdate")
    )
}
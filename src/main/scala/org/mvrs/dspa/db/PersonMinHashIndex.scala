package org.mvrs.dspa.db

import java.util.Base64

import com.sksamuel.elastic4s.http.ElasticDsl.{binaryField, dateField}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexWithUpsertOutputFormat, ElasticSearchNode}

/**
  * Index of minhash signature per person
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class PersonMinHashIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexWithUpsertOutputFormat[(Long, MinHashSignature)](indexName, nodes) {

  override def getDocumentId(record: (Long, MinHashSignature)): String = record._1.toString // person id

  /**
    * Creates the document to insert
    *
    * @param record tuple of person id and MinHash signature
    * @return document map
    */
  override def createDocument(record: (Long, MinHashSignature)): Map[String, Any] =
    Map(
      "minhash" -> Base64.getEncoder.encodeToString(record._2.bytes),
      "lastUpdate" -> System.currentTimeMillis()
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      binaryField("minhash").index(false), // minhash signature, base64 encoded
      dateField("lastUpdate")
    )
}

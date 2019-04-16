package org.mvrs.dspa.recommendations.staticdata

import java.util.Base64

import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class MinHashOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, MinHashSignature)](uri, indexName, typeName) {

  override def getId(record: (Long, MinHashSignature)): String = record._1.toString

  override def getFields(record: (Long, MinHashSignature)): Iterable[(String, Any)] =
    Iterable(
      "minhash" -> Base64.getEncoder.encodeToString(record._2.bytes),
      "lastUpdate" -> System.currentTimeMillis()
    )
}

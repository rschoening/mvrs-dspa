package org.mvrs.dspa.jobs.recommendations.staticdata

import java.util.Base64

import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.{ElasticSearchNode, ElasticSearchUpsertOutputFormat}

class MinHashOutputFormat(indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchUpsertOutputFormat[(Long, MinHashSignature)](indexName, typeName, nodes: _*) {

  override def getId(record: (Long, MinHashSignature)): String = record._1.toString

  override def getFields(record: (Long, MinHashSignature)): Iterable[(String, Any)] =
    Iterable(
      "minhash" -> Base64.getEncoder.encodeToString(record._2.bytes),
      "lastUpdate" -> System.currentTimeMillis()
    )
}

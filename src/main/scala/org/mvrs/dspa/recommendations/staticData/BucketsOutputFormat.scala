package org.mvrs.dspa.recommendations.staticData

import java.util.Base64

import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, Seq[(Long, MinHashSignature)])](uri, indexName, typeName) {

  override def getId(record: (Long, Seq[(Long, MinHashSignature)])): String = record._1.toString

  override def getFields(record: (Long, Seq[(Long, MinHashSignature)])): Iterable[(String, Any)] =
    Iterable(
      "users" -> record._2.map(t => Map(
        "uid" -> t._1,
        "minhash" -> Base64.getEncoder.encodeToString(t._2.bytes))),
      "lastUpdate" -> System.currentTimeMillis()
    )
}
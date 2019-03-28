package org.mvrs.dspa.recommendations.staticData

import java.util.Base64

import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, List[(Long, MinHashSignature)])](uri, indexName, typeName) {

  override def getId(record: (Long, List[(Long, MinHashSignature)])): String = record._1.toString

  override def getFields(record: (Long, List[(Long, MinHashSignature)])): Iterable[(String, Any)] =
    Iterable(
      "users" -> record._2.map(t => Map(
        "uid" -> t._1,
        "minhash" -> Base64.getEncoder.encodeToString(t._2.bytes))),
      "lastUpdate" -> System.currentTimeMillis()
    )
}

class BucketsOutputFormat2(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, List[Long])](uri, indexName, typeName) {

  override def getId(record: (Long, List[Long])): String = record._1.toString

  override def getFields(record: (Long, List[Long])): Iterable[(String, Any)] =
    Iterable(
      "uid" -> record._2,
      "lastUpdate" -> System.currentTimeMillis()
    )
}
package org.mvrs.dspa.recommendations.staticdata

import java.util.Base64

import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, List[Long])](uri, indexName, typeName) {

  override def getId(record: (Long, List[Long])): String = record._1.toString

  override def getFields(record: (Long, List[Long])): Iterable[(String, Any)] =
    Iterable(
      "uid" -> record._2,
      "lastUpdate" -> System.currentTimeMillis()
    )
}
package org.mvrs.dspa.recommendations

import java.util.Base64

import com.sksamuel.elastic4s.http.ElasticClient
import com.twitter.algebird.MinHashSignature
import org.mvrs.dspa.io.ElasticSearchOutputFormat

class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchOutputFormat[(Long, Seq[(Long, MinHashSignature)])](uri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: (Long, Seq[(Long, MinHashSignature)]), client: ElasticClient): Unit = {
    // TODO write minhash per user in separate index
    client.execute {
      update(record._1.toString) in indexName / typeName docAsUpsert(
        "users" -> record._2.map(t => Map(
          "uid" -> t._1,
          "minhash" -> Base64.getEncoder.encodeToString(t._2.bytes))),
        "lastUpdate" -> System.currentTimeMillis())
    }.await
  }
}

package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.ElasticClient
import org.mvrs.dspa.io.ElasticSearchOutputFormat

class FeaturesOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchOutputFormat[(Long, List[String])](uri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: (Long, List[String]), client: ElasticClient): Unit = {
    client.execute {
      update(record._1.toString) in indexName / typeName docAsUpsert(
        "features" -> record._2,
        "lastUpdate" -> System.currentTimeMillis
      )
    }.await

  }
}

package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.ElasticClient
import org.mvrs.dspa.io.ElasticSearchOutputFormat

// TODO add abstract class for upsert (additional ctor parameters: key selector, template method to return field map, no longer expose client)
class KnownUsersOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchOutputFormat[(Long, List[Long])](uri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: (Long, List[Long]), client: ElasticClient): Unit = {
    client.execute {
      update(record._1.toString) in indexName / typeName docAsUpsert(
        "knownUsers" -> record._2,
        "lastUpdate" -> System.currentTimeMillis
      )
    }.await

  }
}

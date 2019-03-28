package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.mvrs.dspa.io.ElasticSearchSinkFunction

class RecommendationsSinkFunction(uri: String, indexName: String, typeName: String)
  extends ElasticSearchSinkFunction[(Long, Seq[(Long, Double)])](uri, indexName, typeName) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: (Long, Seq[(Long, Double)]),
                       client: ElasticClient,
                       context: SinkFunction.Context[_]): Unit = {
    // NOTE: connections are "unexpectedly closed" when using onComplete on the future - need to await
    // TODO: find out how to properly batch and/or do async inserts

    client.execute {
      update(record._1.toString).in(indexName / typeName).docAsUpsert(
        "users" -> record._2.map(t => Map(
          "uid" -> t._1,
          "similarity" -> t._2)),
        "lastUpdate" -> System.currentTimeMillis()
      )
    }.await
  }
}

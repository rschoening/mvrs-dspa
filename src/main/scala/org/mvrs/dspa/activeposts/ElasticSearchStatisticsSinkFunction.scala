package org.mvrs.dspa.activeposts

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.mvrs.dspa.events.PostStatistics
import org.mvrs.dspa.io.ElasticSearchSinkFunction

class ElasticSearchStatisticsSinkFunction(uri: String, indexName: String, typeName: String)
  extends ElasticSearchSinkFunction[PostStatistics](uri, indexName, typeName) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def process(record: PostStatistics, client: ElasticClient, context: SinkFunction.Context[_]): Unit = {
    client.execute {
      indexInto(indexName / typeName)
        .fields("postId" -> record.postId,
          "replyCount" -> record.replyCount,
          "commentCount" -> record.commentCount,
          "likeCount" -> record.likeCount,
          "distinctUserCount" -> record.distinctUserCount,
          "newPost" -> record.newPost,
          "timestamp" -> record.time)
    }.await
  }
}

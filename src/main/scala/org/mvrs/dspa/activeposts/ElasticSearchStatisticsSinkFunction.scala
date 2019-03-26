package org.mvrs.dspa.activeposts

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.mvrs.dspa.events.PostStatistics

class ElasticSearchStatisticsSinkFunction(uri: String, indexName: String, typeName: String) extends RichSinkFunction[PostStatistics] {
  private var client: Option[ElasticClient] = None

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def open(parameters: Configuration): Unit = client = Some(ElasticClient(ElasticProperties(uri)))

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }

  override def invoke(value: PostStatistics, context: SinkFunction.Context[_]): Unit = process(value, client.get, context)


  private def process(record: PostStatistics, client: ElasticClient, context: SinkFunction.Context[_]): Unit = {
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

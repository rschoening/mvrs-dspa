package org.mvrs.dspa.activeposts

import java.util

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

import scala.collection.JavaConverters._

object ActivePostStatisticsIndex {
  def create(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("postId"),
          intField("replyCount").index(false),
          intField("likeCount").index(false),
          intField("commentCount").index(false),
          intField("distinctUserCount").index(false),
          booleanField("newPost").index(false),
          dateField("timestamp")
        )
      )
    }.await

  }

  def createSink(hostname: String, port: Int, scheme: String,
                 indexName: String, typeName: String): ElasticsearchSink[PostStatistics] = {
    val hosts = List(new HttpHost(hostname, port, scheme))

    val esSinkBuilder = new ElasticsearchSink.Builder[PostStatistics](
      hosts.asJava,
      new ElasticsearchSinkFunction[PostStatistics] {
        def createIndexRequest(record: PostStatistics): IndexRequest = {

          new IndexRequest(indexName, typeName).source(createDocument(record))
        }

        override def process(record: PostStatistics, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(record))
        }
      }
    )

    // buffering has a huge influence on performance
    esSinkBuilder.setBulkFlushMaxActions(200)

    //    // provide a RestClientFactory for custom configuration on the internally created REST client
    //    //    esSinkBuilder.setRestClientFactory(
    //    //      restClientBuilder => {
    //    //         restClientBuilder.setDefaultHeaders(...)
    //    //         restClientBuilder.setMaxRetryTimeoutMillis(...)
    //    //         restClientBuilder.setPathPrefix(...)
    //    //         restClientBuilder.setHttpClientConfigCallback(...)
    //    //      }
    //    //    )
    //
    esSinkBuilder.build()
  }

  private def createDocument(record: PostStatistics): util.Map[String, Any] = {
    Map[String, Any](
      "postId" -> record.postId,
      "replyCount" -> record.replyCount,
      "commentCount" -> record.commentCount,
      "likeCount" -> record.likeCount,
      "distinctUserCount" -> record.distinctUserCount,
      "newPost" -> record.newPost,
      "timestamp" -> record.time
    ).asJava
  }
}

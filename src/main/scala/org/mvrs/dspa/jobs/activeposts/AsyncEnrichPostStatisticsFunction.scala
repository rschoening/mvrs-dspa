package org.mvrs.dspa.jobs.activeposts

import com.sksamuel.elastic4s.http.ElasticDsl.{idsQuery, search, _}
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}
import org.mvrs.dspa.model.PostStatistics

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


class AsyncEnrichPostStatisticsFunction(postFeaturesIndexName: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[PostStatistics, (PostStatistics, String)](nodes: _*) {
  override def asyncInvoke(client: ElasticClient,
                           input: PostStatistics,
                           resultFuture: ResultFuture[(PostStatistics, String)]): Unit = {
    import scala.collection.JavaConverters._

    client.execute {
      search(postFeaturesIndexName).query {
        idsQuery(input.postId.toString)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: PostStatistics, response: Response[SearchResponse]): Seq[(PostStatistics, String)] = {
    val content: Array[String] =
      response.result.hits.hits.map(
        _.sourceAsMap("content").asInstanceOf[String])
    assert(content.length <= 1, s"unexpected number of hits for post ${input.postId}")

    List((input, if (content.length == 0) "" else content(0)))
  }

}

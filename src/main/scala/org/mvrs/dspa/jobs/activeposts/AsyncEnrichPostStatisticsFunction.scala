package org.mvrs.dspa.jobs.activeposts

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.mvrs.dspa.jobs.activeposts.AsyncEnrichPostStatisticsFunction.PostInfos
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

/**
  * Async I/O function to retrieve post information from an ElasticSearch index and produce a stream of post statistics
  * enriched with post content and forum title information.
  *
  * @param postInfosIndexName The name of the index containing post information
  * @param nodes              The ElasticSearch nodes to connect to
  */
class AsyncEnrichPostStatisticsFunction(postInfosIndexName: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[PostStatistics, (PostStatistics, String, String), PostInfos, SearchResponse](_.postId.toString, nodes) {

  override protected def getCacheValue(input: PostStatistics, output: (PostStatistics, String, String)): Option[PostInfos] =
    Some(
      PostInfos(
        output._2, // content
        output._3, // forum title
      )
    )

  override protected def toOutput(input: PostStatistics, cachedValue: PostInfos): (PostStatistics, String, String) =
    (input, cachedValue.content, cachedValue.forumTitle)

  override protected def executeQuery(client: ElasticClient, input: PostStatistics): Future[Response[SearchResponse]] =
    client.execute {
      search(postInfosIndexName).query {
        idsQuery(input.postId.toString)
      }
    }

  override protected def unpackResponse(response: Response[SearchResponse], input: PostStatistics): Option[(PostStatistics, String, String)] = {
    val hits = response.result.hits.hits
    if (hits.length == 0) {
      Some((input, "<unknown content>", "<unknown forum>"))
    }
    else {
      assert(hits.length == 1, s"unexpected number of hits for post ${input.postId}")

      val source = hits(0).sourceAsMap

      val content = source("content").asInstanceOf[String]
      val imageFile = source("imageFile").asInstanceOf[String]
      val forumTitle = source("forumTitle").asInstanceOf[String]

      // return tuple
      Some(
        (
          input,
          if (content.isEmpty) imageFile else content,
          forumTitle
        )
      )
    }
  }
}

/**
  * Companion object
  */
object AsyncEnrichPostStatisticsFunction {

  case class PostInfos(content: String, forumTitle: String)

}


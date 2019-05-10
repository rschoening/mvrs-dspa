package org.mvrs.dspa.jobs.activeposts

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

class AsyncForumTitleLookupFunction(forumFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[PostEvent, (PostEvent, String), String](_.forumId.toString, nodes) {

  override protected def getCacheValue(input: PostEvent, output: (PostEvent, String)): String = output._2

  override protected def toOutput(input: PostEvent, cachedValue: String): (PostEvent, String) = (input, cachedValue)

  override protected def executeQuery(client: ElasticClient, input: PostEvent): Future[Response[SearchResponse]] =
    client.execute {
      search(forumFeaturesIndex).query {
        idsQuery(input.forumId.toString)
      }
    }

  override def unpackResponse(response: Response[SearchResponse], input: PostEvent): (PostEvent, String) = {
    val hits = response.result.hits.hits

    if (hits.length == 0) {
      (input, "<unknown forum>")
    }
    else {
      assert(hits.length == 1)

      val source = hits(0).sourceAsMap
      val title = source("title").asInstanceOf[String]

      (input, title)
    }
  }
}




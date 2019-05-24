package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

/**
  * Async I/O function for looking up forum features from ElasticSEarch, based on incoming post events, and producing an
  * output stream of (post event, forum features) tuples.
  *
  * @param forumFeaturesIndex The name of the ElasticSearch index containing the forum features.
  * @param nodes              The elastic search nodes to connect to
  */
class AsyncForumFeaturesLookupFunction(forumFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[PostEvent, (PostEvent, Set[String]), Set[String], SearchResponse](_.forumId.toString, nodes) {
  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the value to cache, which must be serializable
    */
  override protected def getCacheValue(input: PostEvent, output: (PostEvent, Set[String])): Set[String] = output._2

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  override protected def toOutput(input: PostEvent, cachedValue: Set[String]): (PostEvent, Set[String]) = (input, cachedValue)

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  override protected def executeQuery(client: ElasticClient, input: PostEvent): Future[Response[SearchResponse]] =
    client.execute {
      search(forumFeaturesIndex).query {
        idsQuery(input.forumId.toString)
      }
    }

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  override protected def unpackResponse(response: Response[SearchResponse], input: PostEvent): Option[(PostEvent, Set[String])] = {
    val hits = response.result.hits.hits

    if (hits.length == 0) {
      Some((input, Set()))
    }
    else {
      assert(hits.length == 1)

      val source = hits(0).sourceAsMap

      Some((input, source("features").asInstanceOf[List[String]].toSet))
    }
  }
}
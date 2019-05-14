package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala._
import org.mvrs.dspa.model.ForumEvent
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

class AsyncPersonMinHashLookupFunction(personFeaturesIndex: String, personFeaturesType: String, minHasher: MinHasher32, nodes: Seq[ElasticSearchNode], maximumCacheSize: Long = 1000)
  extends AsyncCachingElasticSearchFunction[ForumEvent, (Long, MinHashSignature), MinHashSignature, GetResponse](_.personId.toString, nodes, maximumCacheSize) {

  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the value to cache, which must be serializable
    */
  override protected def getCacheValue(input: ForumEvent, output: (Long, MinHashSignature)): MinHashSignature = output._2

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  override protected def toOutput(input: ForumEvent, cachedValue: MinHashSignature): (Long, MinHashSignature) = (input.personId, cachedValue)

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  override protected def executeQuery(client: ElasticClient, input: ForumEvent): Future[Response[GetResponse]] =
    client.execute {
      get(input.personId.toString) from personFeaturesIndex / personFeaturesType
    }

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  override protected def unpackResponse(response: Response[GetResponse], input: ForumEvent): Option[(Long, MinHashSignature)] =
    if (response.result.found)
      Option(
        (
          input.personId,
          RecommendationUtils.getMinHashSignature(getFeatures(response.result), minHasher)
        )
      )
    else None

  private def getFeatures(response: GetResponse): Iterable[String] = response.source("features").asInstanceOf[Iterable[String]]
}

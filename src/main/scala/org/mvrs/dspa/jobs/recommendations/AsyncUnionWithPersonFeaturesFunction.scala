package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

class AsyncUnionWithPersonFeaturesFunction(personFeaturesIndex: String, personFeaturesType: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[(Long, Set[String]), (Long, Set[String]), Set[String], GetResponse](_._1.toString, nodes) {


  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the value to cache, which must be serializable
    */
  override protected def getCacheValue(input: (Long, Set[String]), output: (Long, Set[String])): Set[String] = output._2

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  override protected def toOutput(input: (Long, Set[String]), cachedValue: Set[String]): (Long, Set[String]) = (input._1, cachedValue)

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  override protected def executeQuery(client: ElasticClient, input: (Long, Set[String])): Future[Response[GetResponse]] =
    client.execute {
      get(input._1.toString) from personFeaturesIndex / personFeaturesType
    }

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  override protected def unpackResponse(response: Response[GetResponse], input: (Long, Set[String])): Option[(Long, Set[String])] =
    if (response.result.found)
      Option(
        (
          input._1,
          input._2 ++ getFeatures(response.result)
        )
      )
    else None

  private def getFeatures(response: GetResponse): Seq[String] = response.source("features").asInstanceOf[List[String]]
}

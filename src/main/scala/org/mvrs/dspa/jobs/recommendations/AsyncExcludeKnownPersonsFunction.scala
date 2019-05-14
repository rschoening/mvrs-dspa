package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.scala._
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

class AsyncExcludeKnownPersonsFunction(knownPersonsIndex: String, knownPersonsType: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[(Long, MinHashSignature, Set[Long]), (Long, MinHashSignature, Set[Long]), Set[Long], GetResponse](_._1.toString, nodes) {

  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the value to cache, which must be serializable
    */
  override protected def getCacheValue(input: (Long, MinHashSignature, Set[Long]),
                                       output: (Long, MinHashSignature, Set[Long])): Set[Long] = input._3

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  override protected def toOutput(input: (Long, MinHashSignature, Set[Long]),
                                  cachedValue: Set[Long]): (Long, MinHashSignature, Set[Long]) = (input._1, input._2, cachedValue)

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  override protected def executeQuery(client: ElasticClient, input: (Long, MinHashSignature, Set[Long])): Future[Response[GetResponse]] =
    client.execute {
      get(input._1.toString) from knownPersonsIndex / knownPersonsType
    }

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  override protected def unpackResponse(response: Response[GetResponse],
                                        input: (Long, MinHashSignature, Set[Long])): Option[(Long, MinHashSignature, Set[Long])] =
    Some(
      (
        input._1, // person id
        input._2, // minhash signature
        if (response.result.found) input._3 -- unpackKnownUsers(response) // subtract the known users from the set
        else input._3
      )
    )

  private def unpackKnownUsers(response: Response[GetResponse]) =
    response.result.sourceAsMap("knownUsers")
      .asInstanceOf[List[Int]]
      .map(_.toLong)
}

package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.model.Recommendation
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.util.{Failure, Success}

class AsyncRecommendUsersFunction(personMinHashIndex: String, minHasher: MinHasher32,
                                  maximumRecommendationCount: Int, minimumRecommendationSimilarity: Double,
                                  nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature, Set[Long]), Recommendation](nodes) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, MinHashSignature, Set[Long]),
                           resultFuture: ResultFuture[Recommendation]): Unit = {
    import scala.collection.JavaConverters._

    val personIds: Set[Long] = input._3

    client.execute {
      search(personMinHashIndex) query {
        idsQuery(personIds)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature, Set[Long]), response: Response[SearchResponse]): List[Recommendation] = {
    val candidates = response.result.hits.hits.map(hit => (hit.id.toLong, decodeMinHash(hit))).toList

    List(
      Recommendation(
        input._1,
        RecommendationUtils.getTopN(
          input._2, candidates, minHasher,
          maximumRecommendationCount, minimumRecommendationSimilarity)
      )
    )
  }

  private def decodeMinHash(hit: SearchHit) = {
    val result = hit.sourceAsMap("minhash").asInstanceOf[String]

    RecommendationUtils.decodeMinHashSignature(result)
  }

}



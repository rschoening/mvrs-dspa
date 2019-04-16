package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.AsyncElasticSearchFunction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncRecommendUsers(elasticSearchUri: String, minHasher: MinHasher32)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature, Set[Long]), (Long, Seq[(Long, Double)])](elasticSearchUri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient, input: (Long, MinHashSignature, Set[Long]), resultFuture: ResultFuture[(Long, Seq[(Long, Double)])]): Unit = {
    import scala.collection.JavaConverters._

    val personIds: Set[Long] = input._3

    client.execute {
      search("recommendation_person_minhash") query {
        idsQuery(personIds)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature, Set[Long]), response: Response[SearchResponse]): List[(Long, Seq[(Long, Double)])] = {
    val candidates = response.result.hits.hits.map(hit => (hit.id.toLong, getMinHash(hit))).toList

    List((input._1, RecommendationUtils.getTopN(input._2, candidates, minHasher, 5, 0.2)))
  }

  // TODO do this outside for testability

  private def getMinHash(hit: SearchHit) = {
    val result = hit.sourceAsMap("minhash").asInstanceOf[String]

    RecommendationUtils.decodeMinHashSignature(result)
  }

}



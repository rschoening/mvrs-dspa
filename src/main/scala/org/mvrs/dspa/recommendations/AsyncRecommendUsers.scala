package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.AsyncElasticSearchFunction
import org.mvrs.dspa.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncRecommendUsers(elasticSearchUri: String, minHasher: MinHasher32)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature, Set[Long]), (Long, Seq[(Long, Double)])](elasticSearchUri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient, input: (Long, MinHashSignature, Set[Long]), resultFuture: ResultFuture[(Long, Seq[(Long, Double)])]): Unit = {
    import scala.collection.JavaConverters._

    // println(s"buckets for ${input._1}: $buckets")
    client.execute {
      search("recommendation_person_minhash").query {
        idsQuery(input._2)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature, Set[Long]), response: Response[SearchResponse]): List[(Long, Seq[(Long, Double)])] = {
    val candidates = response.result.hits.hits.map(hit => (hit.id.toLong, getMinHash(hit))).toList

    List((input._1, getTopN(input._2, candidates, 5, 0.2)))
  }

  private def getTopN(minHashSignature: MinHashSignature,
                      candidates: Seq[(Long, MinHashSignature)],
                      n: Int,
                      minimumSimilarity: Double) = {
    candidates.map(c => (c._1, minHasher.similarity(minHashSignature, c._2)))
      .filter(t => t._2 >= minimumSimilarity)
      .sortBy(-1 * _._2)
      .take(n)
  }

  private def getMinHash(hit: SearchHit) = {
    val result = hit.sourceAsMap("minhash").asInstanceOf[String]

    utils.decodeMinHashSignature(result)
  }

}

package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.AsyncElasticSearchFunction
import org.mvrs.dspa.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncSimilarUsersLookup(elasticSearchUri: String, minHasher: MinHasher32)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature), (Long, Seq[(Long, Double)])](elasticSearchUri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient, input: (Long, MinHashSignature), resultFuture: ResultFuture[(Long, Seq[(Long, Double)])]): Unit = {
    import scala.collection.JavaConverters._

    val buckets = minHasher.buckets(input._2)
    val personId = input._1

    //      // println(s"buckets for ${input._1}: $buckets")
    //      client.execute {
    //        search("recommendation_lsh_buckets").query {
    //          bool {
    //            must {
    //              termsQuery("_id", buckets.map(_.toString))// TODO exclude personId from results
    //            }
    //            not {
    //              termsQuery("_id")
    //            }
    //          }
    //        }
    //      }.onComplete {
    //        case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
    //        case Failure(exception) => resultFuture.completeExceptionally(exception)
    //      }


    // println(s"buckets for ${input._1}: $buckets")
    client.execute {
      search("recommendation_lsh_buckets").query {
        idsQuery(buckets.map(_.toString)) // TODO exclude personId from results (indiv. items, lists that only contain that id)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature), response: Response[SearchResponse]): List[(Long, Seq[(Long, Double)])] = {
    val buckets = response.result.hits.hits.map(hit => unpackHit(hit)).toList

    val personId = input._1
    val candidates = buckets
      .flatMap(bucketResult => bucketResult.filter(user => user._1 != personId))
      .toMap // this removes duplicate keys, keeping the last value written (this is what we want here since the minhashes are unique per person)

    // TODO consider storing minhash value in separate index, as it makes the
    // --> first get all buckets, union sets of users, remove this user, remove known users, remove inactive users
    // --> look up the minhashes for the remaining users

    // TODO exclude known users
    // TODO exclude inactive users

    List((personId, getTopN(input._2, candidates, n = 5, minimumSimilarity = 0.2)))
  }

  private def getTopN(minHashSignature: MinHashSignature,
                      candidates: Map[Long, MinHashSignature],
                      n: Int,
                      minimumSimilarity: Double) = {
    candidates.map(c => (c._1, minHasher.similarity(minHashSignature, c._2)))
      .filter(t => t._2 >= minimumSimilarity)
      .toSeq
      .sortBy(-1 * _._2)
      .take(n)
  }

  private def unpackHit(hit: SearchHit) = {
    val result = hit.sourceAsMap("users").asInstanceOf[List[Map[String, Any]]]
    result.map(t => (t("uid").asInstanceOf[Int].toLong, utils.decodeMinHashSignature(t("minhash").asInstanceOf[String])))
  }

}

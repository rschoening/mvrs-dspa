package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.util.{Failure, Success}

class AsyncCandidateUsersLookupFunction(lshBucketsIndex: String, minHasher: MinHasher32, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature), (Long, MinHashSignature, Set[Long])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, MinHashSignature),
                           resultFuture: ResultFuture[(Long, MinHashSignature, Set[Long])]): Unit = {
    import scala.collection.JavaConverters._

    val buckets = minHasher.buckets(input._2)

    // TODO or use termsQuery to get more compact result?
    client.execute {
      search(lshBucketsIndex) query {
        idsQuery(buckets.map(_.toString))
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature), response: Response[SearchResponse]) = {
    val ids: Set[Long] =
      response.result.hits.hits.flatMap(
        _.sourceAsMap("uid").asInstanceOf[List[Int]].map(_.toLong)) // uids are returned as ints even if in the mapping they are declared as long
        .filter(_ != input._1)
        .toSet

    List((input._1, input._2, ids))
  }
}



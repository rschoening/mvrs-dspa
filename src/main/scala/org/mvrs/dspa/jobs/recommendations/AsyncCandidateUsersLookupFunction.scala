package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.util.{Failure, Success}

/**
  * Async I/O function for looking up candidate persons mapped to the same LSH buckets as the input person.
  *
  * @param lshBucketsIndex The name of the ElasticSearch index containing the LSH buckets and associated person ids
  * @param minHasher       The MinHasher used to calculate the buckets to look up in the index
  * @param nodes           The ElasticSearch nodes to connect to
  */
class AsyncCandidateUsersLookupFunction(lshBucketsIndex: String, minHasher: MinHasher32, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature), (Long, MinHashSignature, Set[Long])](nodes) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, MinHashSignature),
                           resultFuture: ResultFuture[(Long, MinHashSignature, Set[Long])]): Unit = {
    import scala.collection.JavaConverters._

    val buckets = minHasher.buckets(input._2)

    // TODO use termsQuery to get more compact result?
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
        .filter(_ != input._1) // exclude the input person, which is likely in the same bucket also
        .toSet

    List(
      (
        input._1, // person id
        input._2, // MinHash signature of input person
        ids // person ids of candidates from same buckets
      )
    )
  }
}



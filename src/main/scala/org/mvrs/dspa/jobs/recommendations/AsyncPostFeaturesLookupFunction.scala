package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

/**
  * Async I/O function for looking up features based on sets of post ids per person, and
  * emitting a stream of (person id, Set[Feature]) tuples.
  *
  * @param postFeaturesIndex The name of the ElasticSearch index containing the aggregated features per post
  * @param nodes             The elastic search nodes to connect to
  *
  */
class AsyncPostFeaturesLookupFunction(postFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, Set[Long]), (Long, Set[String])](nodes) {

  // TODO add base class for caching for queries based on *sets of* ids, similar to the existing class for single-id queries

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, Set[Long]),
                           resultFuture: ResultFuture[(Long, Set[String])]): Unit = {
    val postIds = input._2

    client.execute {
      search(postFeaturesIndex) sourceFiltering(Seq("features"), Nil) query {
        idsQuery(postIds.map(_.toString))
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, Set[Long]), response: Response[SearchResponse]): Seq[(Long, Set[String])] = {
    val features: Set[String] =
      response.result.hits.hits.flatMap(
        _.sourceAsMap("features").asInstanceOf[List[String]])
        .toSet

    List((input._1, features))
  }
}

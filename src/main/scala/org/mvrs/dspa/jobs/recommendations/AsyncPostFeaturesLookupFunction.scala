package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncPostFeaturesLookupFunction(postFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, Set[Long]), (Long, Set[String])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, Set[Long]),
                           resultFuture: ResultFuture[(Long, Set[String])]): Unit = {
    import scala.collection.JavaConverters._

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

package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncPostFeaturesLookup(nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, mutable.Set[Long]), (Long, Set[String])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient, input: (Long, mutable.Set[Long]), resultFuture: ResultFuture[(Long, Set[String])]): Unit = {
    import scala.collection.JavaConverters._

    val postIds = input._2

    client.execute {
      search("recommendations_posts") sourceFiltering(Seq("features"), Nil) query {
        idsQuery(postIds.map(_.toString))
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, mutable.Set[Long]), response: Response[SearchResponse]): Seq[(Long, Set[String])] = {
    val features: Set[String] =
      response.result.hits.hits.flatMap(
        _.sourceAsMap("features").asInstanceOf[List[String]])
        .toSet

    List((input._1, features))
  }
}

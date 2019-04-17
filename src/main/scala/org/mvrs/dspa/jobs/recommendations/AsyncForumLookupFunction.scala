package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.io.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncForumLookupFunction(forumFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[PostEvent, (PostEvent, Set[String])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: PostEvent,
                           resultFuture: ResultFuture[(PostEvent, Set[String])]): Unit = {
    import scala.collection.JavaConverters._

    client.execute {
      search(forumFeaturesIndex).query {
        idsQuery(input.forumId.toString)
      }
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: PostEvent, response: Response[SearchResponse]): Seq[(PostEvent, Set[String])] = {
    val features: Set[String] =
      response.result.hits.hits.flatMap(
        _.sourceAsMap("features").asInstanceOf[List[String]])
        .toSet

    List((input, features))
  }
}




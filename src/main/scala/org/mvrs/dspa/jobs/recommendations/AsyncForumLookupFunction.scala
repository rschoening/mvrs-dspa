package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.util.{Failure, Success}

class AsyncForumLookupFunction(forumFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[PostEvent, (PostEvent, String, Set[String])](nodes) {

  override def asyncInvoke(client: ElasticClient,
                           input: PostEvent,
                           resultFuture: ResultFuture[(PostEvent, String, Set[String])]): Unit = {
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

  private def unpackResponse(input: PostEvent, response: Response[SearchResponse]): Seq[(PostEvent, String, Set[String])] = {
    val hits = response.result.hits.hits

    if (hits.length == 0) {
      Nil
    }
    else {
      assert(hits.length == 1)

      val source = hits(0).sourceAsMap

      val features = source("features").asInstanceOf[List[String]].toSet
      val title = source("title").asInstanceOf[String]

      List((input, title, features))
    }
  }
}




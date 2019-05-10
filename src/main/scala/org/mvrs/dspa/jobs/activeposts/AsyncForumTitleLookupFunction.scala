package org.mvrs.dspa.jobs.activeposts

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import io.chrisdavenport.read.implicits._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.Cache
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class AsyncForumTitleLookupFunction(forumFeaturesIndex: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[PostEvent, (PostEvent, String)](nodes: _*) with CheckpointedFunction {
  @transient private var listState: ListState[Map[Long, String]] = _
  @transient private lazy val cache = new Cache[Long, String]()

  // TODO metrics for cache misses/hits/cache size

  override def asyncInvoke(client: ElasticClient,
                           input: PostEvent,
                           resultFuture: ResultFuture[(PostEvent, String)]): Unit = {
    import scala.collection.JavaConverters._

    cache.get(input.forumId) match {
      case Some(title) => resultFuture.complete(List((input, title)).asJava)

      case None => client.execute {
        search(forumFeaturesIndex).query {
          idsQuery(input.forumId.toString)
        }
      }.onComplete {
        case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }
  }

  private def unpackResponse(input: PostEvent, response: Response[SearchResponse]): Seq[(PostEvent, String)] = {
    val hits = response.result.hits.hits

    if (hits.length == 0) {
      Nil
    }
    else {
      assert(hits.length == 1)

      val source = hits(0).sourceAsMap
      val title = source("title").asInstanceOf[String]

      cache.put(input.forumId, title)

      List((input, title))
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    listState.clear()
    listState.add(cache.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor[Map[Long, String]](
      "forum-title-by-post-id", createTypeInformation[Map[Long, String]])

    listState = context.getOperatorStateStore.getListState(stateDescriptor)

    if (context.isRestored) {
      cache.clear()
      listState.get().asScala.foreach(cache.putAll)
    }
  }
}




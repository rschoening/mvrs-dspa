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
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.Cache
import org.mvrs.dspa.utils.elastic.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class AsyncEnrichPostStatisticsFunction(postInfosIndexName: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[PostStatistics, (PostStatistics, String, String)](nodes: _*)
    with CheckpointedFunction {

  @transient private var listState: ListState[Map[Long, PostInfos]] = _
  @transient private lazy val cache = new Cache[Long, PostInfos]()

  override def asyncInvoke(client: ElasticClient,
                           input: PostStatistics,
                           resultFuture: ResultFuture[(PostStatistics, String, String)]): Unit =
    cache.get(input.postId) match {
      case Some(PostInfos(content, forumTitle)) => resultFuture.complete(List((input, content + "** CACHE HIT **", forumTitle)).asJava)

      case None => client.execute {
        search(postInfosIndexName).query {
          idsQuery(input.postId.toString)
        }
      }.onComplete {
        case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }

  private def unpackResponse(input: PostStatistics, response: Response[SearchResponse]): Seq[(PostStatistics, String, String)] = {
    val hits = response.result.hits.hits
    if (hits.length == 0) {
      List((input, "<unknown content>", "<unknown forum>"))
    }
    else {
      assert(hits.length == 1, s"unexpected number of hits for post ${input.postId}")

      val source = hits(0).sourceAsMap

      val content = source("content").asInstanceOf[String]
      val imageFile = source("imageFile").asInstanceOf[String]
      val forumTitle = source("forumTitle").asInstanceOf[String]

      cache.put(input.postId, PostInfos(if (content.isEmpty) imageFile else content, forumTitle))

      List((input, content, forumTitle))
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    listState.clear()
    listState.add(cache.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor[Map[Long, PostInfos]](
      "post-infos-by-post-id", createTypeInformation[Map[Long, PostInfos]])

    listState = context.getOperatorStateStore.getListState(stateDescriptor)

    if (context.isRestored) {
      cache.clear()
      listState.get().asScala.foreach(cache.putAll)
    }
  }

  case class PostInfos(content: String, forumTitle: String)

}

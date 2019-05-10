package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import io.chrisdavenport.read.implicits._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.mvrs.dspa.utils.Cache

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

abstract class AsyncCachingElasticSearchFunction[IN, OUT: TypeInformation, V: TypeInformation](getCacheKey: IN => String,
                                                                                               nodes: Seq[ElasticSearchNode],
                                                                                               maximumCacheSize: Long = 10000)
  extends AsyncElasticSearchFunction[IN, OUT](nodes)
    with CheckpointedFunction {

  @transient private var listState: ListState[Map[String, V]] = _
  @transient private lazy val cache = new Cache[String, V](maximumCacheSize)

  protected def getCacheValue(input: IN, output: OUT): V

  protected def toOutput(input: IN, cachedValue: V): OUT

  protected def executeQuery(client: ElasticClient, input: IN): Future[Response[SearchResponse]]

  protected def unpackResponse(response: Response[SearchResponse], input: IN): OUT

  final override def asyncInvoke(client: ElasticClient,
                                 input: IN,
                                 resultFuture: ResultFuture[OUT]): Unit =
    cache.get(getCacheKey(input)) match {
      case Some(value) => resultFuture.complete(List(toOutput(input, value)).asJava)

      case None => executeQuery(client, input).onComplete {
        case Success(response) => resultFuture.complete(List(unpack(response, input)).asJava)
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    listState.clear()
    listState.add(cache.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor("cache-content", createTypeInformation[Map[String, V]])

    listState = context.getOperatorStateStore.getListState(stateDescriptor)

    if (context.isRestored) {
      cache.clear()
      listState.get().asScala.foreach(cache.putAll)
    }
  }

  private def unpack(response: Response[SearchResponse], input: IN) = {
    val output = unpackResponse(response, input)

    val value = getCacheValue(input, output)

    cache.put(getCacheKey(input), value)

    output
  }
}

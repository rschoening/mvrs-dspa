package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import io.chrisdavenport.read.implicits._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Gauge, Meter}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.mvrs.dspa.utils.Cache

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Function base class for asynchronous requests to ElasticSearch that use an LRU cache
  *
  * @param getCacheKey      function to get the cache key (string) from an input element
  * @param nodes            the elastic search nodes
  * @param maximumCacheSize the maximum cache size for the LRU cache
  * @tparam IN  type of input elements
  * @tparam OUT type of output elements
  * @tparam V   type of cached value. This value can be different from the output element, which often also included
  *             the input element (or some transformation of it). The output element is produced based on the
  *             input element and the cached value.
  * @tparam R   the type of ElasticSearch response
  */
abstract class AsyncCachingElasticSearchFunction[IN, OUT: TypeInformation, V: TypeInformation, R](getCacheKey: IN => String,
                                                                                                  nodes: Seq[ElasticSearchNode],
                                                                                                  maximumCacheSize: Long = 10000)
  extends AsyncElasticSearchFunction[IN, OUT](nodes)
    with CheckpointedFunction {

  // metrics
  @transient private var cacheMisses: Counter = _
  @transient private var cacheMissesPerSecond: Meter = _
  @transient private var cacheHits: Counter = _
  @transient private var cacheHitsPerSecond: Meter = _
  @transient private var esimatedCacheSize: Gauge[Long] = _

  // operator state
  @transient private var listState: ListState[Map[String, V]] = _

  // the LRU cache
  @transient private lazy val cache = new Cache[String, V](maximumCacheSize)

  override protected def openCore(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    cacheMisses = group.counter("cacheMisses")
    cacheMissesPerSecond = group.meter("cacheMissesPerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    cacheHits = group.counter("cacheHits")
    cacheHitsPerSecond = group.meter("cacheHitsPerSeconds",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    esimatedCacheSize = group.gauge[Long, ScalaGauge[Long]]("estimatedCacheSize",
      ScalaGauge[Long](() => cache.estimatedSize))
  }

  protected def getCacheValue(input: IN, output: OUT): V

  protected def toOutput(input: IN, cachedValue: V): OUT

  protected def executeQuery(client: ElasticClient, input: IN): Future[Response[R]]

  protected def unpackResponse(response: Response[R], input: IN): OUT

  final override def asyncInvoke(client: ElasticClient,
                                 input: IN,
                                 resultFuture: ResultFuture[OUT]): Unit =
    cache.get(getCacheKey(input)) match {
      case Some(value) =>
        cacheHits.inc()
        cacheHitsPerSecond.markEvent()
        resultFuture.complete(List(toOutput(input, value)).asJava)

      case None =>
        cacheMisses.inc()
        cacheMissesPerSecond.markEvent()
        executeQuery(client, input).onComplete {
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

  private def unpack(response: Response[R], input: IN) = {
    val output = unpackResponse(response, input)

    val value = getCacheValue(input, output)

    cache.put(getCacheKey(input), value)

    output
  }
}

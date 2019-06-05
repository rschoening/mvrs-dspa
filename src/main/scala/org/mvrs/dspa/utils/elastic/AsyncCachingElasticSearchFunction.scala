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
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * Function base class for asynchronous requests to ElasticSearch that use an LRU cache
  *
  * @param getCacheKey        function to get the cache key (string) from an input element
  * @param nodes              the elastic search nodes to connect to
  * @param maximumCacheSize   the maximum cache size for the LRU cache
  * @param cacheEmptyResponse indicates if an empty database response is cached, i.e. the query is not retried if it
  *                           once returned empty for a given cache key.
  * @param checkpointCache    indicates if the cache content should be included in checkpoints. If false, the cache will
  *                           be empty on recovery.
  * @param ttl                the optional time-to-live duration (in processing time) for cached records.
  * @tparam IN  type of input elements
  * @tparam OUT type of output elements
  * @tparam V   type of cached value. This value can be different from the output element, which often also included
  *             the input element (or some transformation of it). The output element is produced based on the
  *             input element and the cached value.
  * @tparam R   the type of ElasticSearch response
  */
abstract class AsyncCachingElasticSearchFunction[IN, OUT: TypeInformation, V: TypeInformation, R](getCacheKey: IN => String,
                                                                                                  nodes: Seq[ElasticSearchNode],
                                                                                                  maximumCacheSize: Long = 10000,
                                                                                                  cacheEmptyResponse: Boolean = false,
                                                                                                  checkpointCache: Boolean = true,
                                                                                                  ttl: Option[Duration] = None)
  extends AsyncElasticSearchFunction[IN, OUT](nodes)
    with CheckpointedFunction {

  // metrics
  @transient private var cacheMisses: Counter = _
  @transient private var cacheMissesPerSecond: Meter = _
  @transient private var cacheHits: Counter = _
  @transient private var cacheHitsPerSecond: Meter = _
  @transient private var estimatedCacheSize: Gauge[Long] = _

  // operator state
  @transient private var listState: ListState[Map[String, Option[V]]] = _

  // the LRU cache
  @transient private lazy val cache = new Cache[String, Option[V]](maximumCacheSize)

  override protected def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    cacheMisses = group.counter("cacheMisses")
    cacheMissesPerSecond = group.meter("cacheMissesPerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    cacheHits = group.counter("cacheHits")
    cacheHitsPerSecond = group.meter("cacheHitsPerSeconds",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    estimatedCacheSize = group.gauge[Long, ScalaGauge[Long]]("estimatedCacheSize",
      ScalaGauge[Long](() => cache.estimatedSize))
  }

  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the optional value to cache, which must be serializable
    */
  protected def getCacheValue(input: IN, output: OUT): Option[V]

  /**
    * Give the subclass a chance to directly derive the output from the input
    *
    * @param input the input element
    * @return the output element to emit if direct conversion is possible, otherwise None
    */
  protected def toOutput(input: IN): Option[OUT] = None

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  protected def toOutput(input: IN, cachedValue: V): OUT

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  protected def executeQuery(client: ElasticClient, input: IN): Future[Response[R]]

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  protected def unpackResponse(response: Response[R], input: IN): Option[OUT]

  override def asyncInvoke(client: ElasticClient,
                           input: IN,
                           resultFuture: ResultFuture[OUT]): Unit =
    toOutput(input) match {
      case Some(output) => // direct conversion possible: add to cache and complete the future
        cache.put(getCacheKey(input), getCacheValue(input, output), ttl)
        resultFuture.complete(List(output).asJava)

      case None => // no direct conversion possible; try to get from cache, else execute the query
        cache.get(getCacheKey(input)) match {
          case Some(value) => // cache hit
            cacheHits.inc()
            cacheHitsPerSecond.markEvent()

            resultFuture.complete(
              value.map(v => List(toOutput(input, v)))
                .getOrElse(Nil)
                .asJava)

          case None => // cache miss
            cacheMisses.inc()
            cacheMissesPerSecond.markEvent()

            executeQuery(client, input).onComplete {
              case Success(response) => resultFuture.complete(
                unpack(response, input)
                  .map(List(_))
                  .getOrElse(Nil)
                  .asJava)

              case Failure(exception) => resultFuture.completeExceptionally(exception)
            }
        }
    }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    listState.clear()

    if (checkpointCache) listState.add(cache.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor("cache-content", createTypeInformation[Map[String, Option[V]]])

    listState = context.getOperatorStateStore.getListState(stateDescriptor)

    if (context.isRestored) {
      cache.clear()
      listState.get().asScala.foreach(cache.putAll)
    }
  }

  private def unpack(response: Response[R], input: IN): Option[OUT] =
    unpackResponse(response, input) match {
      case result@Some(output) =>
        getCacheValue(input, output) match {
          case Some(value) => cache.put(getCacheKey(input), Some(value), ttl)
          case None => // don't cache
        }
        result

      case result@None =>
        if (cacheEmptyResponse) cache.put(getCacheKey(input), None, ttl)
        result
    }
}

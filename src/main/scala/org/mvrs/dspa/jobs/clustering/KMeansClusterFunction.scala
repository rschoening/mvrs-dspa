package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.clustering.KMeansClusterFunction._
import org.mvrs.dspa.jobs.clustering.KMeansClustering.Point
import org.mvrs.dspa.utils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class KMeansClusterFunction(k: Int, decay: Double = 0.9,
                            windowSize: Time, minElementCount: Int, maxElementCount: Int,
                            broadcastStateDescriptor: MapStateDescriptor[String, ClusteringParameter])
  extends KeyedBroadcastProcessFunction[Int, mutable.ArrayBuffer[Double], ClusteringParameter, (Long, Int, ClusterModel)] {
  require(k > 1, s"invalid k: $k")
  require(windowSize.toMilliseconds > 0, s"invalid window size: $windowSize")
  require(minElementCount >= 0, s"invalid minimum element count: $minElementCount")
  require(maxElementCount > 0, s"invalid maximum element count: $maxElementCount")
  require(decay >= 0 && decay <= 1, s"invalid decay: $decay (must be between 0 and 1)")

  private val clusterStateDescriptor = new ValueStateDescriptor("cluster-model", classOf[ClusterModel])
  private val nextTimerStateDescriptor = new ValueStateDescriptor("next-timer", classOf[Long])
  private val windowExtendedStateDescriptor = new ValueStateDescriptor("window-extended", classOf[Boolean])
  private val elementCountStateDescriptor = new ValueStateDescriptor("element-count", classOf[Int])

  private val elementsStateDescriptor = new ListStateDescriptor("elements", classOf[Element])
  private val nextElementsStateDescriptor = new ListStateDescriptor("next-elements", classOf[Element])

  private val LOG = LoggerFactory.getLogger(classOf[KMeansClusterFunction])

  override def processElement(value: ArrayBuffer[Double],
                              ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], ClusteringParameter, (Long, Int, ClusterModel)]#ReadOnlyContext,
                              out: Collector[(Long, Int, ClusterModel)]): Unit = {
    var nextTimer = getRuntimeContext.getState(nextTimerStateDescriptor).value()
    val windowExtended = getRuntimeContext.getState(windowExtendedStateDescriptor).value()
    val elementsListState = getRuntimeContext.getListState(elementsStateDescriptor)
    val nextElementsListState: ListState[Element] = getRuntimeContext.getListState(nextElementsStateDescriptor)
    val elementCountState = getRuntimeContext.getState(elementCountStateDescriptor)

    if (nextTimer == 0) {
      // register the first timer
      nextTimer = ctx.timestamp() + windowSize.toMilliseconds
      registerTimer(nextTimer, ctx.timerService(), ctx.getCurrentKey)
    }

    val windowStartTime = nextTimer - windowSize.toMilliseconds

    if (ctx.timestamp() < windowStartTime) {
      LOG.warn(
        s"Late event ($value): ${utils.formatTimestamp(ctx.timestamp())}, received in window starting at " +
          s"${utils.formatTimestamp(windowStartTime)} (late by " +
          s"${utils.formatDuration(windowStartTime - ctx.timestamp())})")
      // TODO write to side output?
    }
    else if (ctx.timestamp() > nextTimer && !windowExtended) {
      // element with timestamp after next timer, but delivered before the timer
      // (the timer fires only after watermark for its timestamp has passed, so there can be "early" elements
      // belonging to the next window)
      nextElementsListState.add(Element(value))
    }
    else {
      // regular element, add to list state
      elementsListState.add(Element(value))
      elementCountState.update(elementCountState.value + 1)
    }

    if (elementCountState.value() >= maxElementCount ||
      (windowExtended && elementCountState.value() >= minElementCount)) {
      // early firing or extended window:
      // - early: maximum element count within window reached
      // - extended window: minimum size was not reached on regular window end time, is reached now

      // if there is an existing timer registration, delete it
      if (nextTimer > 0) {
        ctx.timerService().deleteEventTimeTimer(nextTimer)
      }

      val params = new Parameters(ctx.getBroadcastState(broadcastStateDescriptor), k, decay)

      emitClusters(elementsListState, nextElementsListState, elementCountState, out, ctx.timestamp(), params)

      registerTimer(ctx.timestamp() + windowSize.toMilliseconds, ctx.timerService(), ctx.getCurrentKey)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], ClusteringParameter, (Long, Int, ClusterModel)]#OnTimerContext,
                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    val elementsListState = getRuntimeContext.getListState(elementsStateDescriptor)
    val nextElementsListState: ListState[Element] = getRuntimeContext.getListState(nextElementsStateDescriptor)
    val elementCountState = getRuntimeContext.getState(elementCountStateDescriptor)

    val elementCount = elementCountState.value()

    LOG.debug("onTimer: {} ({} elements)", timestamp, elementCount)

    if (elementCount > minElementCount) {
      val params = new Parameters(ctx.getBroadcastState(broadcastStateDescriptor), k, decay)

      emitClusters(elementsListState, nextElementsListState, elementCountState, out, timestamp, params)

      registerTimer(timestamp + windowSize.toMilliseconds, ctx.timerService(), ctx.getCurrentKey)
    }
    else {
      // indicate that the window has to be extended since minimum element count for clustering is not reached
      getRuntimeContext.getState(windowExtendedStateDescriptor).update(true)
    }
  }

  override def processBroadcastElement(value: ClusteringParameter,
                                       ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], ClusteringParameter, (Long, Int, ClusterModel)]#Context,
                                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    ctx.getBroadcastState(broadcastStateDescriptor).put(value.key, value)
  }

  private def emitClusters(elementsState: ListState[Element],
                           nextElementsState: ListState[Element],
                           elementCount: ValueState[Int],
                           out: Collector[(Long, Int, ClusterModel)],
                           timestamp: Long,
                           params: Parameters): Unit = {
    require(!timestamp.isNaN && timestamp > 0)

    val points = elementsState.get.asScala.map(e => Point(e.features.toVector)).toSeq

    if (points.isEmpty) {
      // no need to emit anything, as the clusters are stored in broadcast state downstream, and there is no reliance on
      // watermark updates from the broadcast stream
      // NOTE maybe this should be changed, signal empty update to ensure progress
    }
    else {
      val clusterState = getRuntimeContext.getState(clusterStateDescriptor)

      val newClusterModel = cluster(points, Option(clusterState.value()), params)

      out.collect((timestamp, points.size, newClusterModel))

      // update state

      clusterState.update(newClusterModel)

      elementsState.clear()
      elementCount.update(0)

      val nextElements = utils.toSeq(nextElementsState)

      if (nextElements.nonEmpty) {
        elementsState.addAll(nextElements.asJava)
        elementCount.update(nextElements.size)
        nextElementsState.clear()
      }

      // TODO emit information about cluster movement
      //   - as metric?
      //   - on side output stream?
      // - metric seems more appropriate; however the metric will have to be an aggregate (maximum and average distances?)
    }
  }

  private def registerTimer(nextTimer: Long, timerService: TimerService, key: Int): Unit = {
    LOG.debug("Registering timer for {}: {}", key, utils.formatTimestamp(nextTimer))

    timerService.registerEventTimeTimer(nextTimer)

    getRuntimeContext.getState(nextTimerStateDescriptor).update(nextTimer)
    getRuntimeContext.getState(windowExtendedStateDescriptor).update(false)
  }

}


object KMeansClusterFunction {
  implicit private val random: Random = new Random()

  /**
    * calculate cluster model based on new points, the previous model and the decay factor
    *
    * @param points        the points to cluster
    * @param previousModel the previous cluster model (optional)
    * @param params        parameters for the cluster operation
    * @return the new cluster model
    */
  def cluster(points: Seq[Point], previousModel: Option[ClusterModel], params: Parameters): ClusterModel = {
    val initialCentroids: Seq[(Point, Double)] =
      previousModel
        .map(_.clusters.map(c => (c.centroid, c.weight)))
        .getOrElse(KMeansClustering.createRandomCentroids(points, params.k).map((_, 0.0)))

    // TODO with small point sets the size can become < k - check why
    // assert(initialCentroids.size == params.k, s"unexpected centroid count: ${initialCentroids.size} - expected: ${params.k}")

    val clusters =
      KMeansClustering
        .buildClusters(points, initialCentroids, params.k)
        .zipWithIndex
        .map { case ((centroid, weight), index) => Cluster(index, centroid, weight, params.label(index)) }

    previousModel
      .map(_.update(clusters, params.decay))
      .getOrElse(ClusterModel(clusters.toVector))
  }

  final case class Element(features: mutable.ArrayBuffer[Double])

  class Parameters(mapState: ReadOnlyBroadcastState[String, ClusteringParameter], defaultK: Int, defaultDecay: Double) {
    // TODO avoid the downcasts

    /**
      * the number of clusters
      *
      * @return
      */
    def k: Int = Option(mapState.get("k")).map(_.asInstanceOf[ClusteringParameterK].k).getOrElse(defaultK)

    /**
      * The decay factor for the previous cluster model
      *
      * @return
      */
    def decay: Double = Option(mapState.get("decay")).map(_.asInstanceOf[ClusteringParameterDecay].decay).getOrElse(defaultDecay)

    /**
      * labels to associate with clusters
      *
      * @return
      */
    def label(index: Int): Option[String] = Option(mapState.get(s"label$index")).map(_.asInstanceOf[ClusteringParameterLabel].label)
  }

}

package org.mvrs.dspa.jobs.clustering

import javax.annotation.Nonnegative
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.clustering.KMeansClusterFunction._
import org.mvrs.dspa.model
import org.mvrs.dspa.model.{Cluster, ClusterMetadata, ClusterModel, Point}
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Keyed process function to collect featurized events and periodically update and emit the cluster model.
  *
  * @param k                           Initial value for number of clusters (can be altered via control stream)
  * @param decay                       Initial value for decay factor, applied to cluster weights of preceding
  *                                    cluster model; a value of 1.0 results in equal per-point weight of old and
  *                                    new cluster; a value of 0.0
  *                                    ignores the old cluster weight and sets the output weight to the number of
  *                                    points from the current window that were assigned to the cluster.
  * @param windowSize                  The event time size of the tumbling window at the end of which the cluster
  *                                    model is updated
  * @param minElementCount             The minimum element count for updating the cluster model. If not enough elements
  *                                    arrived within the window size, the window is extended until the minimum value
  *                                    is reached.
  * @param maxElementCount             The maximum element count for an update of the cluster model. If this number of
  *                                    elements is reached, the cluster model is updated and emitted, and a new regular
  *                                    window is initiated (count-based early firing)
  * @param broadcastStateDescriptor    The state descriptor for the cluster parameter broadcast state
  * @param outputTagClusters           The output tag for the cluster parameters side output stream
  * @param includeLateElementsInWindow Indicates if elements with event times prior to the current window should be
  *                                    included (true) or discarded (false)
  * @param random                      The optional random number generator for cluster centroids. Default: seeded
  *                                    with nanoTime
  */
class KMeansClusterFunction(@Nonnegative k: Int, @Nonnegative decay: Double = 0.9,
                            windowSize: Time, @Nonnegative minElementCount: Int, @Nonnegative maxElementCount: Int,
                            broadcastStateDescriptor: MapStateDescriptor[String, ClusteringParameter],
                            outputTagClusters: Option[OutputTag[ClusterMetadata]] = None,
                            includeLateElementsInWindow: Boolean = true,
                            random: Random = new Random())
  extends KeyedBroadcastProcessFunction[Int, Vector[Double], ClusteringParameter, (Long, Int, ClusterModel)] {
  require(k > 1, s"invalid k: $k")
  require(windowSize.toMilliseconds > 0, s"invalid window size: $windowSize")
  require(minElementCount >= 0, s"invalid minimum element count: $minElementCount")
  require(maxElementCount > 0, s"invalid maximum element count: $maxElementCount")
  require(decay >= 0 && decay <= 1, s"invalid decay: $decay (must be between 0 and 1)")

  // metrics
  @transient private var aheadOfWindowElementCounter: Counter = _
  @transient private var withinWindowElementCounter: Counter = _
  @transient private var behindWindowElementCounter: Counter = _

  @transient private var earlyFiringCounter: Counter = _
  @transient private var delayedFiringCounter: Counter = _
  @transient private var regularFiringCounter: Counter = _

  @transient private var clusteringTimeMillis: Histogram = _
  @transient private var clusteringPointCount: Histogram = _

  // state
  @transient private lazy val elementsListState = getRuntimeContext.getListState(elementsStateDescriptor)
  @transient private lazy val nextElementsListState = getRuntimeContext.getListState(nextElementsStateDescriptor)

  @transient private lazy val clusterState = getRuntimeContext.getState(clusterStateDescriptor)
  @transient private lazy val nextTimerState = getRuntimeContext.getState(nextTimerStateDescriptor)
  @transient private lazy val windowExtendedState = getRuntimeContext.getState(windowExtendedStateDescriptor)
  @transient private lazy val elementCountState = getRuntimeContext.getState(elementCountStateDescriptor)

  // state descriptors
  // NOTE: use createTypeInformation to ensure serialization by Flink (not Kryo)
  @transient private lazy val elementsStateDescriptor = new ListStateDescriptor("elements", createTypeInformation[Vector[Double]])
  @transient private lazy val nextElementsStateDescriptor = new ListStateDescriptor("next-elements", createTypeInformation[Vector[Double]])

  @transient private lazy val clusterStateDescriptor = new ValueStateDescriptor("cluster-model", createTypeInformation[ClusterModel])
  @transient private lazy val nextTimerStateDescriptor = new ValueStateDescriptor("next-timer", classOf[Long])
  @transient private lazy val windowExtendedStateDescriptor = new ValueStateDescriptor("window-extended", classOf[Boolean])
  @transient private lazy val elementCountStateDescriptor = new ValueStateDescriptor("element-count", classOf[Int])

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[KMeansClusterFunction])

  override def open(parameters: Configuration): Unit = {
    // no TTL configuration needed, since this is run in a single worker and for a single key
    // -> no risk of leaking state

    val group = getRuntimeContext.getMetricGroup

    aheadOfWindowElementCounter = group.counter("aheadOfWindowElementCounter")
    withinWindowElementCounter = group.counter("withinWindowElementCounter")
    behindWindowElementCounter = group.counter("behindWindowElementCounter")

    earlyFiringCounter = group.counter("earlyFiringCounter")
    delayedFiringCounter = group.counter("delayedFiringCounter")
    regularFiringCounter = group.counter("regularFiringCounter") // with name "onTimeFiringCounter" the chart is labelled as having unit "ms"

    clusteringTimeMillis = FlinkUtils.histogramMetric("clusteringTimeMillis", group)
    clusteringPointCount = FlinkUtils.histogramMetric("clusteringPointCount", group)
  }

  override def processElement(value: Vector[Double],
                              ctx: KeyedBroadcastProcessFunction[Int, Vector[Double], ClusteringParameter, (Long, Int, ClusterModel)]#ReadOnlyContext,
                              out: Collector[(Long, Int, ClusterModel)]): Unit = {
    var nextTimer = nextTimerState.value()
    val elementTimestamp = ctx.timestamp()
    val windowStartTime = nextTimer - windowSize.toMilliseconds

    if (nextTimer == 0) {
      // register the first timer
      nextTimer = ctx.timestamp() + windowSize.toMilliseconds
      registerTimer(nextTimer, ctx.timerService(), ctx.getCurrentKey)
    }

    if (elementTimestamp > nextTimer && !windowExtendedState.value()) {
      // element with timestamp after next timer, but delivered before the timer
      // (the timer fires only after watermark for its timestamp has passed, so there can be "early" elements
      // belonging to the next window)
      nextElementsListState.add(value)
      aheadOfWindowElementCounter.inc()
    }
    else if (elementTimestamp < windowStartTime) {
      // late element, event time prior to start of current window.
      if (includeLateElementsInWindow) assignToWindow(value) // include anyway in list state

      warn(
        s"Late event ($value): ${DateTimeUtils.formatTimestamp(elementTimestamp)} before window, received in window starting at " +
          s"${DateTimeUtils.formatTimestamp(windowStartTime)} (late by " +
          s"${DateTimeUtils.formatDuration(windowStartTime - elementTimestamp)})")

      behindWindowElementCounter.inc()
    }
    else {
      // regular element, add to list state
      assignToWindow(value)

      withinWindowElementCounter.inc()
    }

    val maxElementCountReached = elementCountState.value() >= maxElementCount
    val delayedWindowReadyToEmit = windowExtendedState.value() && elementCountState.value() >= minElementCount

    if (maxElementCountReached || delayedWindowReadyToEmit) {
      // early firing or extended window:
      // - early: maximum element count within window reached
      // - extended window: minimum size was not reached on regular window end time, is reached now

      // if there is an existing timer registration, delete it
      if (nextTimer > 0) {
        ctx.timerService().deleteEventTimeTimer(nextTimer)
      }

      val params = new Parameters(ctx.getBroadcastState(broadcastStateDescriptor), k, decay)

      emitClusters(elementsListState, nextElementsListState, elementCountState, out, elementTimestamp, params,
        emitClusterMetadata(_, (tag, metadata) => ctx.output(tag, metadata)))

      registerTimer(elementTimestamp + windowSize.toMilliseconds, ctx.timerService(), ctx.getCurrentKey)

      if (maxElementCountReached) earlyFiringCounter.inc()
      else delayedFiringCounter.inc()
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Int, Vector[Double], ClusteringParameter, (Long, Int, ClusterModel)]#OnTimerContext,
                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    val elementCount = elementCountState.value()

    debug(s"onTimer: $timestamp ($elementCount elements)")

    if (elementCount > minElementCount) {
      val params = new Parameters(ctx.getBroadcastState(broadcastStateDescriptor), k, decay)

      emitClusters(elementsListState, nextElementsListState, elementCountState, out, timestamp, params,
        emitClusterMetadata(_, (tag, metadata) => ctx.output(tag, metadata)))

      regularFiringCounter.inc()

      registerTimer(timestamp + windowSize.toMilliseconds, ctx.timerService(), ctx.getCurrentKey)
    }
    else {
      // indicate that the window has to be extended since minimum element count for clustering is not reached
      windowExtendedState.update(true)
    }
  }

  override def processBroadcastElement(value: ClusteringParameter,
                                       ctx: KeyedBroadcastProcessFunction[Int, Vector[Double], ClusteringParameter, (Long, Int, ClusterModel)]#Context,
                                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    ctx.getBroadcastState(broadcastStateDescriptor).put(value.key, value)
  }

  /**
    * Includes the given element in the window state
    *
    * @param value the element (feature vector) to include
    */
  private def assignToWindow(value: Vector[Double]): Unit = {
    elementsListState.add(value)
    elementCountState.update(elementCountState.value + 1)
  }

  private def emitClusterMetadata(metadata: ClusterMetadata,
                                  output: (OutputTag[ClusterMetadata], ClusterMetadata) => Unit): Unit =
    outputTagClusters.foreach(output(_, metadata))

  /**
    * Update clusters and emit the new cluster model
    *
    * @param elementsState       the list state containing the vectors to cluster
    * @param nextElementsState   the list state containing the vectors belonging to the subsequent window
    * @param elementCount        the running count of elements for the window
    * @param out                 the collector to emit cluster model as tuple (timestamp, point count, cluster model)
    * @param timestamp           the timestamp of the window for regular cluster updates, or the timestamp of the
    *                            triggering element for early or delayed firings
    * @param params              the cluster parameters received from the control stream
    * @param emitClusterMetadata the procedure to emit cluster metadata
    */
  private def emitClusters(elementsState: ListState[Vector[Double]],
                           nextElementsState: ListState[Vector[Double]],
                           elementCount: ValueState[Int],
                           out: Collector[(Long, Int, ClusterModel)],
                           timestamp: Long,
                           params: Parameters,
                           emitClusterMetadata: ClusterMetadata => Unit): Unit = {
    require(!timestamp.isNaN && timestamp > 0)

    val startMillis = System.currentTimeMillis()

    val points = elementsState.get.asScala.map(Point(_)).toSeq

    if (points.isEmpty) {
      // no need to emit anything, as the clusters are stored in broadcast state downstream, and there is no reliance on
      // watermark updates from the broadcast stream
      // NOTE maybe this should be changed, signal empty update to ensure progress
    }
    else {
      val previousModel = Option(clusterState.value())

      val newClusterModel = cluster(points, previousModel, params, random)

      out.collect((timestamp, points.size, newClusterModel))

      // update state

      clusterState.update(newClusterModel)

      elementsState.clear()
      elementCount.update(0)

      val nextElements = FlinkUtils.toSeq(nextElementsState)

      if (nextElements.nonEmpty) {
        elementsState.addAll(nextElements.asJava)
        elementCount.update(nextElements.size)
        nextElementsState.clear()
      }

      emitClusterMetadata(createMetadata(newClusterModel, previousModel, timestamp))
    }

    val duration = System.currentTimeMillis() - startMillis
    debug(s"cluster duration: $duration ms for ${points.size} points")

    clusteringTimeMillis.update(duration)
    clusteringPointCount.update(points.size)
  }

  private def registerTimer(nextTimer: Long, timerService: TimerService, key: Int): Unit = {
    LOG.debug("Registering timer for {}: {}", key, DateTimeUtils.formatTimestamp(nextTimer))

    timerService.registerEventTimeTimer(nextTimer)

    nextTimerState.update(nextTimer)
    windowExtendedState.update(false)
  }

  private def warn(msg: => String): Unit = if (LOG.isWarnEnabled()) LOG.warn(msg)

  private def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)
}

/**
  * Companion object
  */
object KMeansClusterFunction {
  /**
    * calculate cluster model based on new points, the previous model and the decay factor
    *
    * @param points        the points to cluster
    * @param previousModel the previous cluster model (optional)
    * @param params        parameters for the cluster operation
    * @return the new cluster model
    */
  def cluster(points: Seq[Point], previousModel: Option[ClusterModel], params: Parameters, random: Random): ClusterModel = {
    val initialCentroids: Seq[(Point, (Int, Double))] =
      previousModel
        .map(_.clusters.map(c => (c.centroid, (c.index, c.weight))))
        .getOrElse(
          KMeansClustering
            .createRandomCentroids(points, params.k)
            .zipWithIndex // initialize cluster index
            .map { case (centroid, index) => (centroid, (index, 0.0)) })

    val clusters =
      KMeansClustering
        .buildClusters(points, initialCentroids, params.k)(random)
        .map { case (centroid, (index, weight)) => Cluster(index, centroid, weight, params.label(index)) }

    previousModel
      .map(_.update(clusters, params.decay))
      .getOrElse(model.ClusterModel(clusters.toVector))
  }

  /**
    * Derives the cluster metadata for a clustering update
    *
    * @param newClusterModel the new cluster model
    * @param previousModel   the previous cluster model
    * @param timestamp       the timestamp for the cluster update
    * @return cluster metadata
    */
  def createMetadata(newClusterModel: ClusterModel, previousModel: Option[ClusterModel], timestamp: Long): ClusterMetadata = {
    require(newClusterModel.clusters.nonEmpty, "no clusters in new model")

    val prevByIndex: Map[Int, Cluster] = previousModel.map(_.clusters.map(c => (c.index, c)).toMap).getOrElse(Map())

    val newClustersWithDifferences: Vector[(Cluster, Vector[Double], Double, Double)] =
      newClusterModel.clusters.map(
        cluster => prevByIndex.get(cluster.index) match {
          // return tuple (cluster, difference vector, difference vector length, weight difference)
          case Some(prevCluster) =>
            val diff = (cluster.centroid - prevCluster.centroid).features
            (
              cluster,
              diff,
              math.sqrt(diff.map(d => d * d).sum),
              cluster.weight - prevCluster.weight
            )
          case None => (cluster, cluster.centroid.features, 0.0, cluster.weight)
        }
      )

    val avgVectorDifference = newClustersWithDifferences.map { case (_, _, length, _) => length }.sum / newClustersWithDifferences.size
    val avgWeightDifference = newClustersWithDifferences.map { case (_, _, _, weight) => weight }.sum / newClustersWithDifferences.size
    val kDifference = newClusterModel.clusters.size - previousModel.map(_.clusters.size).getOrElse(0)

    ClusterMetadata(
      timestamp,
      newClustersWithDifferences,
      avgVectorDifference,
      avgWeightDifference,
      kDifference
    )
  }


  /**
    * Cluster parameters
    *
    * @param mapState     the broadcast state to retrieve the parameters from
    * @param defaultK     the default value for k (number of clusters)
    * @param defaultDecay the default decay factor
    */
  class Parameters(mapState: ReadOnlyBroadcastState[String, ClusteringParameter], defaultK: Int, defaultDecay: Double) {
    /**
      * the number of clusters
      *
      * @return
      */
    def k: Int = getValue[ClusteringParameterK, Int]("k", _.k).getOrElse(defaultK)

    /**
      * The decay factor for the previous cluster model
      *
      * @return
      */
    def decay: Double = getValue[ClusteringParameterDecay, Double]("decay", _.decay).getOrElse(defaultDecay)

    /**
      * labels to associate with clusters
      *
      * @return
      */
    def label(index: Int): Option[String] = getValue[ClusteringParameterLabel, String](s"label$index", _.label)

    private def getValue[P, V](name: String, value: P => V): Option[V] = Option(mapState.get(name)).map(p => value(p.asInstanceOf[P]))
  }

}

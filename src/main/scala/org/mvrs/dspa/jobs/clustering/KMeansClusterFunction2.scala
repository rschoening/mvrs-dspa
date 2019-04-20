package org.mvrs.dspa.jobs.clustering

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.clustering.KMeansClusterFunction2._
import org.mvrs.dspa.jobs.clustering.KMeansClustering.Point
import org.mvrs.dspa.utils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KMeansClusterFunction2(var k: Int, var decay: Double = 0.9,
                             maxWindowSize: Time, minElementCount: Int, maxElementCount: Int,
                             broadcastStateDescriptor: MapStateDescriptor[String, String])
  extends KeyedBroadcastProcessFunction[Int, mutable.ArrayBuffer[Double], (String, String), (Long, Int, ClusterModel)] with CheckpointedFunction {
  require(k > 1, s"invalid k: $k")
  require(decay >= 0 && decay <= 1, s"invalid decay: $decay (must be between 0 and 1)")

  private val clusterStateDescriptor = new ValueStateDescriptor("cluster-model", classOf[ClusterModel])
  private val nextTimerStateDescriptor = new ValueStateDescriptor("next-timer", classOf[Long])
  private val elements = mutable.ArrayBuffer[Element]()

  @transient private var elementsListState: ListState[Element] = _ // elements in operator state

  override def processElement(value: ArrayBuffer[Double],
                              ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], (String, String), (Long, Int, ClusterModel)]#ReadOnlyContext,
                              out: Collector[(Long, Int, ClusterModel)]): Unit = {
    // if there is a timer set, delete it
    val nextTimer = getRuntimeContext.getState(nextTimerStateDescriptor).value()

    if (nextTimer == 0) {
      registerTimer(ctx.timestamp() + maxWindowSize.toMilliseconds, ctx.timerService())
    }
    else if (ctx.timestamp() > nextTimer) {
      println(s"LATER ELEMENT BEFORE TIMER (by ${utils.formatDuration(ctx.timestamp() - nextTimer)}) - ${utils.formatTimestamp(ctx.timestamp())} - watermark: ${utils.formatTimestamp(ctx.currentWatermark())}")
    }
    else {
     // println(s"REGULAR ELEMENT BEFORE TIMER (by ${utils.formatDuration(nextTimer - ctx.timestamp())}) - ${utils.formatTimestamp(ctx.timestamp())} - watermark: ${utils.formatTimestamp(ctx.currentWatermark())}")
    }

    elements += Element(value)

    if (elements.size >= maxElementCount) {
      if (nextTimer > 0) {
        ctx.timerService().deleteEventTimeTimer(nextTimer)
      }

      emitClusters(out, ctx.timestamp())

      registerTimer(ctx.timestamp() + maxWindowSize.toMilliseconds, ctx.timerService())
    }
  }

  private def emitClusters(out: Collector[(Long, Int, ClusterModel)], timestamp: Long): Unit = {
    val points = elements.map(e => Point(e.features.toVector))

    if (points.isEmpty) {
      // no need to emit anything, as the clusters are stored in broadcast state downstream, and there is no reliance on
      // watermark updates from the broadcast stream
    }
    else
    {
      val clusterState = getRuntimeContext.getState(clusterStateDescriptor)

      val newClusterModel = cluster(points, Option(clusterState.value()), decay, k)

      out.collect((timestamp, points.size, newClusterModel))

      clusterState.update(newClusterModel)
      elements.clear()

      // TODO emit information about cluster movement
      //   - as metric?
      //   - on side output stream?
      // - metric seems more appropriate; however the metric will have to be an aggregate (maximum and average distances?)
    }
  }

  private def registerTimer(nextTimer: Long, timerService: TimerService): Unit = {
    println(s"Registering timer: ${utils.formatTimestamp(nextTimer)}")
    timerService.registerEventTimeTimer(nextTimer)
    getRuntimeContext.getState(nextTimerStateDescriptor).update(nextTimer)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], (String, String), (Long, Int, ClusterModel)]#OnTimerContext,
                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    println("onTimer")
    if (elements.size >= minElementCount) {
      emitClusters(out, timestamp)

    }
    // TODO when to check back if minimum is reached?
    registerTimer(timestamp + maxWindowSize.toMilliseconds, ctx.timerService())
  }

  override def processBroadcastElement(value: (String, String),
                                       ctx: KeyedBroadcastProcessFunction[Int, ArrayBuffer[Double], (String, String), (Long, Int, ClusterModel)]#Context,
                                       out: Collector[(Long, Int, ClusterModel)]): Unit = {
    ctx.getBroadcastState(broadcastStateDescriptor).put(value._1, value._2)

    // TODO validate
    value match {
      case ("k", v) => k = v.toInt
      case ("decay", v) => decay = v.toDouble
      case _ => // ignore
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    elementsListState.clear()
    elements.foreach(elementsListState add _)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val elementsStateDescriptor = new ListStateDescriptor("elements", createTypeInformation[Element])

    elementsListState = context.getOperatorStateStore.getListState(elementsStateDescriptor)

    if (context.isRestored) {
      elements.clear()
      elements ++= elementsListState.get.asScala
    }
  }

  final case class Element(features: mutable.ArrayBuffer[Double])

}


object KMeansClusterFunction2 {
  /**
    * calculate cluster model based on new points, the previous model and the decay factor
    *
    * @param points        the points to cluster
    * @param previousModel the previous cluster model (optional)
    * @param decay         the decay factor for the previous cluster model
    * @param k             the number of clusters
    * @return the new cluster model
    */
  def cluster(points: Seq[Point], previousModel: Option[ClusterModel], decay: Double, k: Int): ClusterModel = {
    val initialCentroids =
      previousModel
        .map(
          _.clusters
            .map(_.centroid)
            .take(k))
        .getOrElse(KMeansClustering.createRandomCentroids(points, k))

    assert(initialCentroids.size == k)

    val clusters =
      KMeansClustering
        .buildClusters(points, initialCentroids)
        .zipWithIndex
        .map { case ((centroid, clusterPoints), index) => Cluster(index, centroid, clusterPoints.size) }

    previousModel
      .map(_.update(clusters, decay))
      .getOrElse(ClusterModel(clusters.toVector))
  }
}

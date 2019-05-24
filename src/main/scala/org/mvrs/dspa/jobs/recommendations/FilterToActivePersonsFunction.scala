package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Gauge}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.recommendations.FilterToActivePersonsFunction._
import org.mvrs.dspa.utils.FlinkUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Filters the candidate set in in input stream of (person id, MinHashsignature, Set[candidate person id]) to those
  * persons that have been active (posting, liking, commenting) within a given time window
  *
  * === Counters:===
  *
  *    1. cleanupCount (Counter): The number of cleanups to evict old entries from the activity map (person id -> timestamp of last activity)
  *    1. cleanupDifferenceHistogram (Histogram): Histogram of number of entries removed from the map per cleanup
  *    1. mapSize (Gauge): the current size of the activity map
  *
  * @param activityTimeout              The timeout for activities to consider, in event time and relative to the event
  *                                     time of the current recommendation
  * @param minimumCleanupIntervalMillis Minimum interval for pruning persons with older latest activities
  * @note a broadcast process function with operator state (union list) is preferred over a windowed join since the
  *       "join" is not symmetric: on incoming primary tuples, a state table populated from the broadcast stream must
  *       be queried for the last activity timestamp of each candidate person.
  */
class FilterToActivePersonsFunction(activityTimeout: Time, minimumCleanupIntervalMillis: Long = 1000)
  extends BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]
    with CheckpointedFunction {

  private val timeoutMillis = activityTimeout.toMilliseconds

  @transient private lazy val lastActivityPerPerson: mutable.Map[Long, Long] = mutable.Map()
  @transient private var lastCleanup: Long = 0L
  @transient private var lastWatermark: Long = 0L

  // metrics
  @transient private var cleanupCounter: Counter = _
  @transient private var cleanupDifferenceHistogram: DropwizardHistogramWrapper = _
  @transient private var mapSize: Gauge[Long] = _

  // NOTE a ttl configuration on broadcast state was initially tried. That does not work because
  // 1) broadcast state is expected to be identical for all operators, to allow redistribution on recover/scaling
  // 2) ttl state configuration seems not to be used on broadcast state
  @transient private var listState: ListState[(Long, Long)] = _ // union list state

  override def open(parameters: Configuration): Unit = {
    // metrics
    val group = getRuntimeContext.getMetricGroup
    cleanupCounter = group.counter("cleanupCount")
    cleanupDifferenceHistogram = FlinkUtils.histogramMetric("cleanupDifferenceHistogram", group)
    mapSize = group.gauge[Long, ScalaGauge[Long]]("mapSize",
      ScalaGauge[Long](() => lastActivityPerPerson.size))
  }

  override def processElement(value: (Long, MinHashSignature, Set[Long]),
                              ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#ReadOnlyContext,
                              out: Collector[(Long, MinHashSignature, Set[Long])]): Unit = {
    out.collect(
      (
        value._1, // person id
        value._2, // signature
        value._3.filter(personId => isActive(personId, ctx.timestamp())) // filtered to active users
      )
    )

    // remove old entries (earlier than the activity timeout before the input watermark)

    // this needs to be done on the primary input, not the broadcast stream, since the timeout relative to the
    // timestamp of the target user for the recommendation is of interest
    val watermark = ctx.currentWatermark()

    if (watermark > lastWatermark) {
      // the watermark has advanced
      evictOldEntries(watermark)
      lastWatermark = watermark
    }
  }

  /**
    * Evict entries from the activity map with an activity timestamp that is earlier than the current watermark minus the
    * activity timeout.
    *
    * @param watermark The current watermark of the primary (non-broadcast) stream, which is timestamped based on the
    *                  sliding window for gathering recent post interactions per person.
    * @note The various Async I/O functions applied to this stream may result in re-ordering of output tuples.
    * @todo clarify if the reordering in Async I/O functions may result in late events for non-late input streams. If
    *       this is the case, an additional tolerance probably has to be added to the timeout (which is however
    *       expressed in event time, i.e. the replay scaling also has to be taken into account)
    */
  private def evictOldEntries(watermark: Long): Unit = {
    val now = System.currentTimeMillis()
    val originalSize = lastActivityPerPerson.size

    if (now - lastCleanup >= minimumCleanupIntervalMillis) {
      lastCleanup = now
      lastActivityPerPerson.retain { case (_, lastActivityTimestamp) => isWithinTimeout(lastActivityTimestamp, watermark) }

      // metrics
      cleanupCounter.inc()
      cleanupDifferenceHistogram.update(originalSize - lastActivityPerPerson.size)
    }
  }

  override def processBroadcastElement(value: Long,
                                       ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#Context,
                                       out: Collector[(Long, MinHashSignature, Set[Long])]): Unit =
    lastActivityPerPerson(value) = ctx.timestamp() // add to map

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    listState.clear()
    // just add the map entries to the list. On recovery, all operators will receive the concatenation of these lists
    listState.addAll(lastActivityPerPerson.toSeq.asJava)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor[(Long, Long)]("lastActivity", createTypeInformation[(Long, Long)])

    listState = context.getOperatorStateStore.getUnionListState(stateDescriptor)
    if (context.isRestored) {
      lastActivityPerPerson.clear()
      getLastActivityByPerson(listState.get().asScala)
        .foreach {
          case (personId, timestamp) => lastActivityPerPerson(personId) = timestamp
        }
    }
  }

  /**
    * Indicates if the given person is considered active as of a given current timestamp, i.e. it has a recorded
    * activity in the map which is within the timeout period before the current timestamp.
    *
    * @param personId      The person id
    * @param asOfTimestamp The current timestamp
    * @return `true` if the person is considered active, `false` otherwise
    *
    */
  private def isActive(personId: Long, asOfTimestamp: Long): Boolean =
    lastActivityPerPerson.get(personId).exists(lastActivityTime => isWithinTimeout(lastActivityTime, asOfTimestamp))

  private def isWithinTimeout(lastActivityTimestamp: Long, currentTimestamp: Long): Boolean =
  // NOTE there are cases where the last activity (from broadcast stream) is later than the current timestamp
  // (defined by the window end times on the main i.e. non-broadcast stream), due to the slower processing chain on the
  // main stream. Still safe though.
    currentTimestamp - lastActivityTimestamp <= timeoutMillis
}

object FilterToActivePersonsFunction {
  def getLastActivityByPerson(activityTimestamps: Iterable[(Long, Long)]): Map[Long, Long] =
    activityTimestamps
      .groupBy(_._1) // -> [person id, Iterable[(person id, timestamp)]
      .mapValues(_.map(_._2).max) // keep only the newest timestamp
}

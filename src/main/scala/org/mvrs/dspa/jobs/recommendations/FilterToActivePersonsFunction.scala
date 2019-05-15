package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.recommendations.FilterToActivePersonsFunction._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Filters the candidate set to those persons that have been active (posting, liking, commenting)
  *
  * @param activityTimeout              the timeout for activities to consider, in event time and relative to the event
  *                                     time of the current recommendation
  * @param minimumCleanupIntervalMillis minimum interval for pruning persons with older latest activities
  */
class FilterToActivePersonsFunction(activityTimeout: Time, minimumCleanupIntervalMillis: Long = 1000)
  extends BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]
    with CheckpointedFunction {

  private val timeoutMillis = activityTimeout.toMilliseconds

  @transient private lazy val lastActivityPerPerson: mutable.Map[Long, Long] = mutable.Map()
  @transient private var lastCleanup: Long = 0L
  @transient private var lastWatermark: Long = 0L

  // NOTE a ttl configuration on broadcast state was initially tried. That does not work because
  // 1) broadcast state is expected to be identical for all operators, to allow redistribution on recover/scaling
  // 2) ttl state configuration seems not to be used on broadcast state
  @transient private var listState: ListState[(Long, Long)] = _ // union list state

  override def processElement(value: (Long, MinHashSignature, Set[Long]),
                              ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#ReadOnlyContext,
                              out: Collector[(Long, MinHashSignature, Set[Long])]): Unit = {
    out.collect(
      (
        value._1, // person id
        value._2, // signature
        value._3.filter(lastActivityPerPerson.get(_).exists(isWithinTimeout(_, ctx.timestamp()))) // filtered to active users
      )
    )

    // remove old entries (earlier than the activity timeout before the input watermark)

    // this needs to be done on the primary input, not the broadcast stream, since the timeout relative to the
    // timestamp of the target user for the recommendation is of interest
    if (ctx.currentWatermark() > lastWatermark) {
      // the watermark has advanced

      val now = System.currentTimeMillis()
      val size = lastActivityPerPerson.size // original size

      if (size > 100 && now - lastCleanup >= minimumCleanupIntervalMillis) {
        lastCleanup = now
        lastActivityPerPerson.retain { case (_, lastActivityTimestamp) => isWithinTimeout(lastActivityTimestamp, ctx.currentWatermark()) }

        // TODO replace with metrics on cache size / cleanup rate etc
        println(s"CLEANED UP: $size -> ${lastActivityPerPerson.size}")
      }
      lastWatermark = ctx.currentWatermark()
    }
  }

  override def processBroadcastElement(value: Long,
                                       ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#Context,
                                       out: Collector[(Long, MinHashSignature, Set[Long])]): Unit =
    lastActivityPerPerson(value) = ctx.timestamp() // add to map

  private def isWithinTimeout(lastActivityTimestamp: Long, currentTimestamp: Long): Boolean =
  // NOTE there are cases where the last activity (from broadcast stream) is later than the current timestamp
  // (defined by the window end times on the main i.e. non-broadcast stream), due to the slower processing chain on the
  // main stream. Still safe though.
    currentTimestamp - lastActivityTimestamp <= timeoutMillis

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
}

object FilterToActivePersonsFunction {
  def getLastActivityByPerson(activityTimestamps: Iterable[(Long, Long)]): Map[Long, Long] =
    activityTimestamps
      .groupBy(_._1) // -> [person id, Iterable[(person id, timestamp)]
      .mapValues(_.map(_._2).max) // keep only the newest timestamp
}

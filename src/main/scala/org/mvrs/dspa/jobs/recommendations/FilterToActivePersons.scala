package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

class FilterToActivePersons(activityTimeoutMillis: Long, // NOTE: flink's old Time class (streaming api) is not serializable
                            lastActivityStateDescriptor: MapStateDescriptor[Long, Long])
  extends BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])] {

  override def processElement(value: (Long, MinHashSignature, Set[Long]),
                              ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#ReadOnlyContext,
                              out: Collector[(Long, MinHashSignature, Set[Long])]): Unit = {
    val state = ctx.getBroadcastState(lastActivityStateDescriptor)

    out.collect(
      (
        value._1, // person id
        value._2, // signature
        value._3.filter(candidate => isWithinTimeout(state.get(candidate), ctx.timestamp())) // filtered to active users
      )
    )
  }

  override def processBroadcastElement(value: Long,
                                       ctx: BroadcastProcessFunction[(Long, MinHashSignature, Set[Long]), Long, (Long, MinHashSignature, Set[Long])]#Context,
                                       out: Collector[(Long, MinHashSignature, Set[Long])]): Unit =
    ctx.getBroadcastState(lastActivityStateDescriptor).put(value, ctx.timestamp()) // remember the last activity time

  private def isWithinTimeout(lastActivityTimestamp: Long, currentTimestamp: Long) =
  // NOTE there are cases where the last activity (from broadcast stream) is later than the current timestamp
  // (defined by the window end times on the main i.e. non-broadcast stream), due to the slower processing chain on the
  // main stream. Still safe though.
    currentTimestamp - lastActivityTimestamp <= activityTimeoutMillis

}

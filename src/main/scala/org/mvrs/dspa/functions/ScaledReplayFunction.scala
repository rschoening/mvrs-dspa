package org.mvrs.dspa.functions

import java.util.Random

import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.functions.ScaledReplayFunction._
import org.mvrs.dspa.utils.FlinkUtils


class ScaledReplayFunction[K, I](extractEventTime: I => Long,
                                 speedupFactor: Double,
                                 maximumDelayMillis: Long,
                                 delay: I => Long, expectOrdered: Boolean) extends KeyedProcessFunction[K, I, I] with CheckpointedFunction {
  // TODO this must be checkpointed, otherwise pending events are lost on failure recovery
  // TODO how to implement split/merge of this state?
  private lazy val scheduler = new EventScheduler[I](speedupFactor, None, maximumDelayMillis, delay, expectOrdered)

  def this(extractEventTime: I => Long, speedupFactor: Double, expectOrdered: Boolean) =
    this(extractEventTime, speedupFactor, 0, (_: I) => 0, expectOrdered)

  def this(extractEventTime: I => Long, speedupFactor: Double, maximumDelayMilliseconds: Long, expectOrdered: Boolean) =
    this(extractEventTime, speedupFactor, maximumDelayMilliseconds,
      (_: I) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds)(),
      expectOrdered)

  override def processElement(value: I, ctx: KeyedProcessFunction[K, I, I]#Context, out: Collector[I]): Unit = {
    val eventTime: Long = extractEventTime(value)
    scheduler.schedule(value, eventTime)

    processPending(out, ctx.timerService())
  }

  private def processPending(out: Collector[I], timerService: TimerService): Unit = {
    val upcomingEventTime: Option[Long] = scheduler.processPending(
      (e, _) => out.collect(e),
      _ => (), // cannot emit watermark outside of source function
      sleep,
      () => false,
      flush = false)

    upcomingEventTime.foreach(registerTimer(_, timerService))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, I, I]#OnTimerContext, out: Collector[I]): Unit = {
    scheduler.updateMaximumEventTime(timestamp)

    processPending(out, ctx.timerService())
  }

  private def sleep(waitTime: Long): Unit = if (waitTime > 0) Thread.sleep(waitTime)

  private def registerTimer(time: Long, timerService: TimerService): Unit = {
    timerService.registerEventTimeTimer(time)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // TODO
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // TODO
  }
}

object ScaledReplayFunction {
  private val rand = new Random(137)

}

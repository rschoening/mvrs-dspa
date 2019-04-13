package org.mvrs.dspa.functions

import java.util.Random

import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.functions.ScaledReplayFunction._
import org.mvrs.dspa.utils


class ScaledReplayFunction[K, I](extractEventTime: I => Long,
                                 speedupFactor: Double,
                                 maximumDelayMillis: Long,
                                 delay: I => Long) extends KeyedProcessFunction[K, I, I] {
  @volatile private lazy val scheduler = new EventScheduler[I](speedupFactor, 10000, maximumDelayMillis, delay)

  def this(extractEventTime: I => Long, speedupFactor: Double) = this(extractEventTime, speedupFactor, 0, (_: I) => 0)

  def this(extractEventTime: I => Long, speedupFactor: Double, maximumDelayMilliseconds: Long) =
    this(extractEventTime, speedupFactor, maximumDelayMilliseconds,
      (_: I) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override def processElement(value: I, ctx: KeyedProcessFunction[K, I, I]#Context, out: Collector[I]): Unit = {
    val eventTime: Long = extractEventTime(value)
    scheduler.schedule(value, eventTime)

    processPending(out, ctx.timerService())
  }

  private def processPending(out: Collector[I], timerService: TimerService): Unit = {
    val headTime = scheduler.processPending(
      (e, _) => out.collect(e),
      _ => (),
      sleep,
      () => false,
      flush = false)

    headTime.foreach(registerTimer(_, timerService))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, I, I]#OnTimerContext, out: Collector[I]): Unit = {
    // TODO let scheduler know about this timestamp?
    scheduler.maximumEventTime = math.max(scheduler.maximumEventTime, timestamp)
    processPending(out, ctx.timerService())
  }

  private def sleep(waitTime: Long): Unit = if (waitTime > 0) Thread.sleep(waitTime)

  private def registerTimer(time: Long, timerService: TimerService): Unit = {
    timerService.registerEventTimeTimer(time)
  }

}

object ScaledReplayFunction {
  private val rand = new Random(137)

}

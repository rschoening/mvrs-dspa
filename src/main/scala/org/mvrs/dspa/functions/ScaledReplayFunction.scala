package org.mvrs.dspa.functions

import java.util.Random

import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.functions.ScaledReplayFunction._

import scala.collection.mutable


class ScaledReplayFunction[I](extractEventTime: I => Long, speedupFactor: Double, maximumDelayMillis: Long, delay: I => Long) extends ProcessFunction[I, I] {
  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[DelayedEvent[I]](Ordering.by((_: DelayedEvent[I]).delayedEventTime).reverse)
  private val timers: mutable.Set[Long] = mutable.Set()
  private var firstEventTime = Long.MinValue
  private var maximumEventTime = Long.MinValue

  def this(extractEventTime: I => Long, speedupFactor: Double) = this(extractEventTime, speedupFactor, 0, (_: I) => 0)

  def this(extractEventTime: I => Long, speedupFactor: Double, maximumDelayMilliseconds: Long) =
    this(extractEventTime, speedupFactor, maximumDelayMilliseconds, (_: I) => getNormalDelayMillis(rand, maximumDelayMilliseconds))

  // assumes monotonically increasing input event times

  // scenario:
  // - max delay = 10
  // 1. event at 100, to be delayed by 10 --> 110
  // 2. event at 101, to be delayed by 1 --> 102
  // 3. event at 102, to be delayed by 1 --> 103
  // 4. event at 103, to be delayed by 1 --> 104
  // --> replay order: 2. 3. 4. 1.
  // at 1: add to queue
  override def processElement(value: I, ctx: ProcessFunction[I, I]#Context, out: Collector[I]): Unit = {
    val eventTime: Long = extractEventTime(value)
    assert(eventTime != Long.MinValue, s"invalid event time: $eventTime")
    assert(eventTime >= maximumEventTime, s"event time $eventTime < maximum event time $maximumEventTime")

    maximumEventTime = eventTime

    val delayMillis = delay(value)
    assert(delayMillis <= maximumDelayMillis, s"delay $delayMillis exceeds maximum $maximumDelayMillis")

    queue += DelayedEvent(eventTime + delayMillis, value)

    if (firstEventTime == Long.MinValue) firstEventTime = eventTime

    processQueue(out, ctx.timerService())

    // NOPE does not work, timers only supported on keyed streams
   // if (queue.nonEmpty) registerTimer(queue.head.delayedEventTime, ctx.timerService())
  }

  private def registerTimer(time: Long, timerService: TimerService): Unit = {
    timers += time
    timerService.registerEventTimeTimer(time)
  }

  private def processQueue(out: Collector[I], timerService: TimerService): Unit = {
    while (queue.nonEmpty && queue.head.delayedEventTime <= maximumEventTime) {
      val head = queue.dequeue()

      deleteTimer(head.delayedEventTime, timerService)

      val replayTime = toReplayTime(replayStartTime, firstEventTime, head.delayedEventTime, speedupFactor)
      val waitTime = replayTime - System.currentTimeMillis()
      println(s"replay time: $replayTime - delayed event time: ${head.delayedEventTime} - wait time: $waitTime - event: ${head.event}")
      Thread.sleep(if (waitTime > 0) waitTime else 0)

      out.collect(head.event)
    }

    // NOPE does not work, timers only supported on keyed streams
    if (queue.nonEmpty) registerTimer(queue.head.delayedEventTime, timerService)

  }

  private def deleteTimer(time: Long, timerService: TimerService): Unit = {
    if (timers.contains(time)) {
      timerService.deleteEventTimeTimer(time)
      timers -= time
    }
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[I, I]#OnTimerContext, out: Collector[I]): Unit = {
    timers -= timestamp
    maximumEventTime = math.max(maximumEventTime, timestamp)
    processQueue(out, ctx.timerService())
  }

}

object ScaledReplayFunction {
  private val rand = new Random(137)

  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }

  def getNormalDelayMillis(rand: Random, maximumDelayMilliseconds: Long): Long = {
    var delay = -1L
    val x = maximumDelayMilliseconds / 2
    while (delay < 0 || delay > maximumDelayMilliseconds) {
      delay = (rand.nextGaussian * x).toLong + x
    }
    delay
  }

  private case class DelayedEvent[I](delayedEventTime: Long, event: I)

}

package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils
import org.slf4j.LoggerFactory

import scala.util.Random

abstract class ReplayedSourceFunction[IN, OUT](parse: IN => OUT,
                                               extractEventTime: OUT => Long,
                                               speedupFactor: Double,
                                               maximumDelayMillis: Int,
                                               delay: OUT => Long,
                                               watermarkIntervalMillis: Long) extends RichSourceFunction[OUT] {
  @volatile private lazy val scheduler = new EventScheduler[OUT](speedupFactor, watermarkIntervalMillis, maximumDelayMillis, delay)

  @volatile private var isRunning = true

  private val LOG = LoggerFactory.getLogger(classOf[ReplayedSourceFunction[IN, OUT]])

  protected def this(parse: IN => OUT,
                     extractEventTime: OUT => Long,
                     speedupFactor: Double = 0,
                     maximumDelayMilliseconds: Int = 0,
                     watermarkInterval: Long = 1000) =
    this(parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds),
      watermarkInterval)


  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    for (input <- inputIterator.takeWhile(_ => isRunning)) {
      val event = parse(input)
      val eventTime = extractEventTime(event) // NOT scaled to replay time

      assert(eventTime > Long.MinValue, s"invalid event time: $eventTime ($event)")

      scheduler.schedule(event, eventTime)

      processPending(ctx)
    }

    processPending(ctx, flush = true)

    LOG.info("replay ended")
  }

  protected def inputIterator: Iterator[IN]

  private def processPending(ctx: SourceFunction.SourceContext[OUT], flush: Boolean = false): Unit = {
    scheduler.processPending(
      (e, timestamp) => ctx.collectWithTimestamp(e, timestamp),
      wm => ctx.emitWatermark(wm),
      () => isRunning,
      flush)
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}


object ReplayedSourceFunction {
  private[functions] val rand = new Random(137)

  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    require(speedupFactor > 0, s"invalid speedup factor: $speedupFactor")

    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }
}
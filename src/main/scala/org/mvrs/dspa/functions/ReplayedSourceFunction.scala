package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils.FlinkUtils
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  *
  * @param parse                   function to convert from input element to output element
  * @param extractEventTime        function to extract event time from output element
  * @param speedupFactor           the speedup factor relative to the event times
  * @param maximumDelayMillis      the upper bound to the expected delays (non-late elements). The emitted watermark will be based on this value.
  * @param delay                   the function to determine the delay based on a parsed element. For unit testing, a function that
  *                                returns defined delays for given elements can be used.
  * @param watermarkIntervalMillis the interval for emitting watermarks
  * @tparam IN  the type of input elements (read from the concrete source as defined in the subclass)
  * @tparam OUT the type of output elements
  */
abstract class ReplayedSourceFunction[IN, OUT](parse: IN => OUT,
                                               extractEventTime: OUT => Long,
                                               speedupFactor: Double,
                                               maximumDelayMillis: Int,
                                               delay: OUT => Long,
                                               watermarkIntervalMillis: Long) extends RichSourceFunction[OUT] {
  @transient private lazy val scheduler = new EventScheduler[OUT](speedupFactor, Some(watermarkIntervalMillis), maximumDelayMillis, delay)
  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[ReplayedSourceFunction[IN, OUT]])
  @transient private var isCancelled = false
  @transient private var rowIndex: Int = _

  protected def this(parse: IN => OUT,
                     extractEventTime: OUT => Long,
                     speedupFactor: Double = 0,
                     maximumDelayMilliseconds: Int = 0,
                     watermarkInterval: Long = 1000) =
    this(parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds),
      watermarkInterval)

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    rowIndex = 0

    for (input <- inputIterator.takeWhile(_ => !isCancelled)) {
      scheduleInput(input, rowIndex)
      processPending(ctx)

      rowIndex += 1
    }

    processPending(ctx, flush = true)
  }

  private def scheduleInput(input: IN, rowIndex: Int): Unit = {
    try {
      val event = parse(input)
      val eventTime = extractEventTime(event)

      assert(eventTime > Long.MinValue, s"invalid event time: $eventTime ($event)")

      scheduler.schedule(event, eventTime)
    }
    catch {
      case e: Throwable => reportRowError(e) // ... and ignore input
    }
  }

  protected def reportRowError(e: Throwable): Unit = {
    LOG.warn(s"error processing row $rowIndex: ${e.getMessage}")
  }

  protected def inputIterator: Iterator[IN]

  private def processPending(ctx: SourceFunction.SourceContext[OUT], flush: Boolean = false): Unit = {
    scheduler.processPending(
      (e, timestamp) => ctx.collectWithTimestamp(e, timestamp),
      ctx.emitWatermark,
      sleep,
      () => isCancelled,
      flush)
  }

  private def sleep(waitTime: Long): Unit = if (waitTime > 0) Thread.sleep(waitTime)

  override def cancel(): Unit = {
    isCancelled = true
  }
}

object ReplayedSourceFunction {
  private[functions] val rand = new Random(137)
}
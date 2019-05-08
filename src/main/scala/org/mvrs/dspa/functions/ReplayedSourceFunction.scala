package org.mvrs.dspa.functions

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Gauge, Meter}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils.FlinkUtils
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Base class for source functions that replay events from an underlying source (defined in the subclass), optionally
  * applying a speedup factor relative to the event times, and
  *
  * @param parse                   function to convert from input element to output element
  * @param extractEventTime        function to extract event time from output element
  * @param speedupFactor           the speedup factor relative to the event times
  * @param maximumDelayMillis      the upper bound for the expected delays (non-late elements). The emitted watermark values will be based on this value.
  * @param delay                   the function to determine the delay based on a parsed element. For unit testing, a function that
  *                                returns defined delays for given elements can be used.
  * @param watermarkIntervalMillis the interval for emitting watermarks, at scale of event time (watermarks will be
  *                                inserted into the event-time based schedule based on this value)
  * @tparam IN  the type of input elements (read from the concrete source as defined in the subclass)
  * @tparam OUT the type of output elements
  */
abstract class ReplayedSourceFunction[IN, OUT](parse: IN => OUT,
                                               extractEventTime: OUT => Long,
                                               speedupFactor: Double,
                                               maximumDelayMillis: Long,
                                               delay: OUT => Long,
                                               watermarkIntervalMillis: Long,
                                               minimumWatermarkEmitIntervalMillis: Long) extends RichSourceFunction[OUT] {
  @transient private lazy val scheduler = new EventScheduler[OUT](
    speedupFactor,
    Some(watermarkIntervalMillis),
    maximumDelayMillis,
    delay,
    expectOrderedInput = true,
    allowLateEvents = true, // delay() return values are allowed to exceed maximumDelayMillis
    minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis
  )

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[ReplayedSourceFunction[IN, OUT]])
  @transient private var isCancelled = false
  @transient private var rowIndex: Int = _

  @transient private var watermarkCounter: Counter = _
  @transient private var watermarkMeter: Meter = _
  @transient private var scheduleLength: Gauge[Int] = _
  @transient private var scheduledEvents: Gauge[Int] = _
  @transient private var scheduledWatermarks: Gauge[Int] = _

  protected def this(parse: IN => OUT,
                     extractEventTime: OUT => Long,
                     speedupFactor: Double = 0,
                     maximumDelayMilliseconds: Int = 0,
                     watermarkIntervalMillis: Int = 10000,
                     minimumWatermarkEmitIntervalMillis: Int = 1000) =
    this(
      parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds),
      watermarkIntervalMillis,
      minimumWatermarkEmitIntervalMillis
    )

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {

    val group = getRuntimeContext.getMetricGroup

    watermarkCounter = group.counter("numWatermarks")
    watermarkMeter = group.meter("numWatermarksPerSecond", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
    scheduleLength = group.gauge[Int, ScalaGauge[Int]]("scheduleLength", ScalaGauge[Int](() => scheduler.scheduleLength))
    scheduledEvents = group.gauge[Int, ScalaGauge[Int]]("scheduledEvents", ScalaGauge[Int](() => scheduler.scheduledEvents))
    scheduledWatermarks = group.gauge[Int, ScalaGauge[Int]]("scheduledWatermarks", ScalaGauge[Int](() => scheduler.scheduledWatermarks))

    for (input <- inputIterator.takeWhile(_ => !isCancelled)) {
      scheduleInput(input, rowIndex)
      processPending(ctx)

      rowIndex += 1
    }

    processPending(ctx, flush = true)

    LOG.info(s"emitted watermarks: ${watermarkCounter.getCount}")
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
      emitWatermark(_, ctx),
      sleep,
      () => isCancelled,
      flush)
  }

  private def emitWatermark(wm: Watermark, ctx: SourceFunction.SourceContext[OUT]): Unit = {
    watermarkCounter.inc()
    watermarkMeter.markEvent()

    ctx.emitWatermark(wm)
  }

  private def sleep(waitTime: Long): Unit = if (waitTime > 0) Thread.sleep(waitTime)

  override def cancel(): Unit = {
    isCancelled = true
  }
}

object ReplayedSourceFunction {
  private[functions] val rand = new Random(137)
}
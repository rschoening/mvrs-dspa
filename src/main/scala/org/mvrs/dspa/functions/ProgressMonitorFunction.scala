package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Counter, Meter}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.functions.ProgressMonitorFunction.ProgressInfo
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}

class ProgressMonitorFunction[I]() extends ProcessFunction[I, (I, ProgressInfo)] {
  // metrics
  @transient private var elementCounter: Counter = _
  @transient private var noWatermarkCounter: Counter = _
  @transient private var behindNewestCounter: Counter = _
  @transient private var lateElementsCounter: Counter = _
  @transient private var watermarkAdvancedCounter: Counter = _
  @transient private var watermarkAdvancedPerSecond: Meter = _
  @transient private var watermarkIncrementHistogram: DropwizardHistogramWrapper = _
  @transient private var latenessHistogram: DropwizardHistogramWrapper = _
  @transient private var behindNewestHistogram: DropwizardHistogramWrapper = _

  // State (not checkpointed, as this function is currently debug-only and does not need to be fault-tolerant).
  // Maximum values represent values since last restore. Checkpointing can be easily added if ever needed.
  @transient private var maximumTimestamp: Long = _
  @transient private var maximumLateness: Long = _
  @transient private var maximumBehindNewest: Long = _
  @transient private var maximumWatermarkIncrement: Long = _
  @transient private var maxElementsSinceWatermarkAdvanced: Long = _
  @transient private var previousWatermark: Long = _
  @transient private var elementsSinceWatermarkAdvanced: Long = 0

  @transient private var startTimeNanos: Long = _
  @transient private var previousWatermarkEmitTimeNanos: Long = _
  @transient private var previousEventEmitTimeNanos: Long = _

  @transient private var sumOfProcTimeBetweenEventsMillis: Double = 0.0
  @transient private var sumOfProcTimeBetweenWatermarksMillis: Double = 0.0

  @transient private lazy val nanosPerMilli: Double = 1000.0 * 1000.0 // constant to convert to milliseconds

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    elementCounter = group.counter("elementCounter") // equal to standard counter
    noWatermarkCounter = group.counter("noWatermarkCounter")
    behindNewestCounter = group.counter("behindNewestCounter")
    lateElementsCounter = group.counter("lateEventCounter")
    watermarkAdvancedCounter = group.counter("watermarkAdvancedCounter")

    watermarkAdvancedPerSecond = group.meter("watermarkAdvancedPerSecond",
      new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    watermarkIncrementHistogram = FlinkUtils.histogramMetric("watermarkIncrementHistogram", group)
    latenessHistogram = FlinkUtils.histogramMetric("latenessHistogram", group)
    behindNewestHistogram = FlinkUtils.histogramMetric("behindNewestHistogram", group)

    previousWatermarkEmitTimeNanos = Long.MinValue
    previousEventEmitTimeNanos = Long.MinValue
    previousWatermark = Long.MinValue
    startTimeNanos = System.nanoTime()
  }

  override def processElement(value: I,
                              ctx: ProcessFunction[I, (I, ProgressInfo)]#Context,
                              out: Collector[(I, ProgressInfo)]): Unit = {
    val elementTimestamp = ctx.timestamp()
    val watermark = ctx.timerService().currentWatermark()

    val isLate = elementTimestamp <= watermark // TODO revert if observed again - elementTimestamp < watermark // actually, <=
    val isBehindNewest = elementTimestamp < maximumTimestamp
    val watermarkAdvanced = watermark > previousWatermark
    val hasPreviousWatermark = previousWatermark != Long.MinValue
    val watermarkIncrement = if (watermarkAdvanced && hasPreviousWatermark) watermark - previousWatermark else 0L
    val lateness = if (watermark == Long.MinValue) 0 else math.max(watermark - elementTimestamp, 0)
    val behindNewest = maximumTimestamp - elementTimestamp

    elementsSinceWatermarkAdvanced = if (watermarkAdvanced) 1 else elementsSinceWatermarkAdvanced + 1

    // maxima
    maximumBehindNewest = math.max(maximumBehindNewest, behindNewest)
    maximumLateness = math.max(maximumLateness, lateness)
    maximumTimestamp = math.max(elementTimestamp, maximumTimestamp)
    maximumWatermarkIncrement = math.max(watermarkIncrement, maximumWatermarkIncrement)
    maxElementsSinceWatermarkAdvanced = math.max(elementsSinceWatermarkAdvanced, maxElementsSinceWatermarkAdvanced)

    // metrics
    elementCounter.inc()
    if (isBehindNewest) behindNewestCounter.inc()
    if (isLate) lateElementsCounter.inc()
    if (watermark == Long.MinValue) noWatermarkCounter.inc()
    if (watermarkAdvanced) watermarkAdvancedPerSecond.markEvent()
    if (watermarkAdvanced) watermarkAdvancedCounter.inc()

    // NOTE histograms appear to not work reliably for display in Flink dashboard
    if (watermarkAdvanced && hasPreviousWatermark) watermarkIncrementHistogram.update(watermarkIncrement)
    latenessHistogram.update(lateness)
    behindNewestHistogram.update(behindNewest)

    // emit time differences
    val nowNanos = System.nanoTime()

    if (previousEventEmitTimeNanos != Long.MinValue) {
      val nanosSincePreviousEvent = nowNanos - previousEventEmitTimeNanos
      sumOfProcTimeBetweenEventsMillis = sumOfProcTimeBetweenEventsMillis + nanosSincePreviousEvent / nanosPerMilli
    }
    previousEventEmitTimeNanos = nowNanos

    if (watermarkAdvanced) {
      if (hasPreviousWatermark) {
        val nanosSincePreviousWatermark = nowNanos - previousWatermarkEmitTimeNanos
        sumOfProcTimeBetweenWatermarksMillis = sumOfProcTimeBetweenWatermarksMillis + nanosSincePreviousWatermark / nanosPerMilli
      }

      previousWatermark = watermark
      previousWatermarkEmitTimeNanos = nowNanos
    }

    val watermarkCount = watermarkAdvancedCounter.getCount
    val elementCount = elementCounter.getCount

    val avgWatermarksPerSecond =
      if (sumOfProcTimeBetweenWatermarksMillis == 0)
        Double.NaN
      else
        watermarkCount.toDouble / (sumOfProcTimeBetweenWatermarksMillis / 1000) // incorrect after restore from checkpoint (metric is checkpointed, sum is not)

    val avgElementsPerSecond =
      if (sumOfProcTimeBetweenEventsMillis == 0)
        Double.NaN
      else
        elementCount.toDouble / (sumOfProcTimeBetweenEventsMillis / 1000) // incorrect after restore from checkpoint (metric is checkpointed, sum is not)

    out.collect(
      (
        value,
        ProgressInfo(
          getRuntimeContext.getIndexOfThisSubtask,
          elementTimestamp,
          watermark,
          watermarkAdvanced,
          watermarkIncrement,
          elementsSinceWatermarkAdvanced,
          maxElementsSinceWatermarkAdvanced,
          maximumTimestamp,
          elementCount,
          lateElementsCounter.getCount,
          behindNewestCounter.getCount,
          noWatermarkCounter.getCount,
          watermarkCount,
          maximumLateness,
          maximumBehindNewest,
          maximumWatermarkIncrement,
          nowNanos - startTimeNanos,
          avgWatermarksPerSecond,
          avgElementsPerSecond
        )
      )
    )
  }
}

/**
  * Companion object
  */
object ProgressMonitorFunction {

  /**
    * Progress information for an element
    *
    * @param subtask                           the subtask (worker) index
    * @param timestamp                         the timestamp of the element
    * @param watermark                         the current watermark at the operator
    * @param watermarkAdvanced                 indicates if the watermark has advanced between the previous element
    *                                          and this element
    * @param watermarkIncrement                the event time difference between the current watermark and the
    *                                          watermark of the previous element
    * @param elementsSinceWatermarkAdvanced    the number of elements observed at the worker since the previous
    *                                          advancement of the watermark
    * @param maxElementsSinceWatermarkAdvanced the maximum number of elements between two subsequent watermarks
    *                                          observed so far at the worker
    * @param maximumTimestamp                  the maximum element timestamp (i.e. the timestamp of the most recent
    *                                          - in event time - element) observed so far at the worker
    * @param elementCount                      the number of total elements observed so far at the worker.
    * @param lateElementsCount                 the number of elements with timestamps that were lower than the watermark
    *                                          timestamp at the operator when the element was received
    * @param elementsBehindNewestCount         the number of elements with a timestamp lower than the maximum timestamp
    * @param noWatermarkCount                  the number of elements received before the arrival of the first watermark
    * @param watermarkAdvancedCount            the number of times that the watermark timestamp has advanced prior to
    *                                          the element
    * @param maximumLateness                   the maximum duration (milliseconds) that an element timestamp was earlier
    *                                          than the watermark timestamp
    * @param maximumBehindNewest               the maximum duration (milliseconds) that an element timestamp was earlier
    *                                          than the maximum timestamp of any element received up to that point
    * @param maximumWatermarkIncrement         the maximum event time difference between two subsequent watermark
    *                                          advancements
    * @param nanosSinceStart                   the time (in nanoseconds) since the (re)start of the task
    * @param avgWatermarksPerSecond            the average number of watermark advancements per second observed so far
    * @param avgElementsPerSecond              the average number of elements per second observed so far
    */
  case class ProgressInfo(subtask: Int,
                          timestamp: Long,
                          watermark: Long,
                          watermarkAdvanced: Boolean,
                          watermarkIncrement: Long,
                          elementsSinceWatermarkAdvanced: Long,
                          maxElementsSinceWatermarkAdvanced: Long,
                          maximumTimestamp: Long,
                          elementCount: Long,
                          lateElementsCount: Long,
                          elementsBehindNewestCount: Long,
                          noWatermarkCount: Long,
                          watermarkAdvancedCount: Long,
                          maximumLateness: Long,
                          maximumBehindNewest: Long,
                          maximumWatermarkIncrement: Long,
                          nanosSinceStart: Long,
                          avgWatermarksPerSecond: Double,
                          avgElementsPerSecond: Double) {
    /**
      * Incidates if the element is late (i.e. has a lower timestamp than the current watermark)
      *
      * @note timestamp == watermark would also be considered late (watermark indicates that no events
      *       with t_event <= t_watermark will be observed) however it seems that the Kafka consumer issues the
      *       watermarks such that this can occur even on ordered topics
      */
    val isLate: Boolean = timestamp < watermark

    /**
      * Indicates if a watermark has been received prior to the element
      */
    val hasWatermark: Boolean = watermark != Long.MinValue

    /**
      * The number of milliseconds the element timestamp is behind the watermark (0 for non-late elements)
      */
    val millisBehindWatermark: Long = math.max(watermark - timestamp, 0)

    /**
      * Indicates if the element has a timestamp that is lower than the maximum element of all previously received
      * elements
      */
    val isBehindNewest: Boolean = timestamp < maximumTimestamp

    /**
      * The number of milliseconds that the timestamp of this element is behind the maximum element of all
      * previously received elements
      */
    val millisBehindNewest: Long = maximumTimestamp - timestamp

    /**
      * The number of milliseconds since the (re)start of the task
      */
    val millisSinceStart: Long = nanosSinceStart / 1000 / 1000

    /**
      * Returns a displayable string representation of the progress information, in a tabular layout.
      * See [ProgressInfo.getSchemaInfo] for the list of columns
      *
      * @return String representation
      */
    override def toString: String = s"[$subtask] " +
      s"ts: ${shortDateTime(timestamp)} " +
      pad(s"| ect: $elementCount", 14) +
      pad(s"| e/s: ${math.round(avgElementsPerSecond)}", 14) +
      pad(s"|| bn: ${if (isBehindNewest) shortDuration(millisBehindNewest) else "latest"}", 17) +
      pad(s"| bnc: $elementsBehindNewestCount", 14) +
      pad(s"| bmx: ${if (maximumBehindNewest == 0) "-" else shortDuration(maximumBehindNewest)}", 18) +
      pad(s"|| lt: ${if (isLate) shortDuration(millisBehindWatermark) else "on time"}", 17) +
      pad(s"| ltc: $lateElementsCount", 13) +
      pad(s"| lmx: ${if (maximumLateness == 0) "-" else shortDuration(maximumLateness)}", 18) +
      pad(s"|| wn: ${if (!hasWatermark) "NO watermark" else shortDateTime(watermark)}", 27) +
      pad(s"${if (watermarkIncrement == 0) "=" else "+" + shortDuration(watermarkIncrement)}", 11) +
      pad(s"| wmx: ${if (maximumWatermarkIncrement == 0) "-" else shortDuration(maximumWatermarkIncrement)}", 18) +
      pad(s"| +wc: $watermarkAdvancedCount", 12) +
      pad(s"| +/s: ${math.round(avgWatermarksPerSecond)}", 13) +
      pad(s"| nwm: $noWatermarkCount", 12) +
      pad(s"|| ew: $elementsSinceWatermarkAdvanced", 14) +
      pad(s"| emx: $maxElementsSinceWatermarkAdvanced", 14) +
      s"|| rt: ${longDuration(millisSinceStart)}"
  }

  object ProgressInfo {
    def getSchemaInfo: String = {
      """------------------------------------------------------------------------------------------------------------------------
        |-  ts :  the timestamp of the element (short format, seconds resolution)
        |- ect : the total number of elements seen so far
        |- e/s : the average number of elements per second (processing time) so far
        |-------
        |-  bn : the number of seconds (in event time) the element is behind the maximum event time seen so far ('latest' if this
        |        element has the highest event time so far)
        |- bnc : the number of elements seen so far that were behined the maximum event time seen when they were received
        |- bmx : the maximum time difference (in event time) that an element seen so far was behind the maximum event time when
        |        it was received
        |-------
        |-  lt : the number of seconds (in event time) the element is late, i.e. that its timestamp is behind the current
        |        watermark ('on time' if the element has a timestamp >= than the current watermark)
        |- ltc : the number of elements seen so far that were late
        |- lmx : the maximum time difference that an element seen so far was behind the watermark wen it was received
        |-------
        |-  wn : the timestamp of the current watermark ('NO watermark') if no watermark has been received yet. If the watermark
        |        has increased, '+' with the watermark time increment is appended to the timestamp. '=' indicates that the
        |        watermark has stayed the same since the last element on the input stream
        |- wmx : the maximum event time value by which the watermark has increased between two observed elements
        |- +wc : the number of times that the watermark timestamp has advanced prior to the element
        |- +/s : the average number of times per second (processing time) that an increase of the watermark timestamp was
        |        observed between two elements
        |- nwm : the number of elements seen that were received before the first watermark
        |-------
        |-  ew : the number of elements seen since the last increase of the watermark
        |- emx : the maximum number of elements for which the watermark stayed the same since the previous increase
        |-------
        |-  rt : the run time since the (re)start of the task
        |------------------------------------------------------------------------------------------------------------------------""".stripMargin
    }
  }

  private def pad(str: String, chars: Int): String = str.padTo(chars, ' ')

  private def shortDateTime(millis: Long): String = DateTimeUtils.formatTimestamp(millis, shortFormat = true)

  private def shortDuration(millis: Long): String = DateTimeUtils.formatDuration(millis, shortFormat = true)

  private def longDuration(millis: Long): String = DateTimeUtils.formatDuration(millis)
}
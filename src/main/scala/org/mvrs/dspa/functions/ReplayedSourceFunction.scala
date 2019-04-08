package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.functions.ScaledReplayFunction.toReplayTime
import org.mvrs.dspa.utils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

abstract class ReplayedSourceFunction[IN, OUT](parse: IN => OUT,
                                               extractEventTime: OUT => Long,
                                               speedupFactor: Double,
                                               maximumDelayMillis: Int,
                                               delay: OUT => Long) extends RichSourceFunction[OUT] {

  private lazy val replayStartTime: Long = System.currentTimeMillis
  private val queue = mutable.PriorityQueue.empty[(Long, Either[OUT, Watermark])](Ordering.by((_: (Long, Either[OUT, Watermark]))._1).reverse)

  private var firstEventTime = Long.MinValue
  private var maximumEventTime = Long.MinValue
  private val minimumWatermarkInterval = 1000
  private val watermarkIntervalMillis = math.max(minimumWatermarkInterval, maximumDelayMillis)

  @volatile private var isRunning = true

  private val LOG = LoggerFactory.getLogger(classOf[ReplayedSourceFunction[IN, OUT]])

  protected def this(parse: IN => OUT,
                     extractEventTime: OUT => Long,
                     speedupFactor: Double = 0,
                     maximumDelayMilliseconds: Int = 0) =
    this(parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    for (input <- inputIterator.takeWhile(_ => isRunning)) {
      val event = parse(input)
      val eventTime = extractEventTime(event)

      assert(eventTime > Long.MinValue, s"invalid event time: $eventTime ($event)")
      assert(eventTime >= maximumEventTime, s"event time $eventTime < maximum event time $maximumEventTime")

      maximumEventTime = eventTime

      val delayMillis = delay(event)
      assert(delayMillis <= maximumDelayMillis, s"delay $delayMillis exceeds maximum $maximumDelayMillis")

      if (firstEventTime == Long.MinValue) {
        firstEventTime = eventTime

        scheduleWatermark(firstEventTime) // schedule first watermark
      }

      queue += ((eventTime + delayMillis, Left(event))) // schedule the event

      processQueue(ctx)
    }

    processQueue(ctx, flush = true)
  }

  protected def inputIterator: Iterator[IN]

  private def processQueue(ctx: SourceFunction.SourceContext[OUT], flush: Boolean = false): Unit = {
    while (queue.nonEmpty && (flush || queue.head._1 <= maximumEventTime)) {
      val head = queue.dequeue()
      val delayedEventTime = head._1

      val replayTime = toReplayTime(replayStartTime, firstEventTime, delayedEventTime, speedupFactor)
      val waitTime = replayTime - System.currentTimeMillis()

      LOG.info(s"replay time: $replayTime - delayed event time: $delayedEventTime - wait time: $waitTime - item: ${head._2}")

      Thread.sleep(if (waitTime > 0) waitTime else 0)

      head._2 match {
        case Left(event) => ctx.collectWithTimestamp(event, extractEventTime(event))
        case Right(watermark) =>
          ctx.emitWatermark(watermark)
          if (isRunning && queue.nonEmpty) scheduleWatermark(delayedEventTime) // schedule next watermark
      }
    }
  }

  private def scheduleWatermark(delayedEventTime: Long): Unit = {
    val watermarkEmitTime = delayedEventTime + watermarkIntervalMillis
    val watermarkEventTime = watermarkEmitTime - maximumDelayMillis - 1
    val nextWatermark = new Watermark(watermarkEventTime)

    queue += ((watermarkEmitTime, Right(nextWatermark)))
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

object ReplayedSourceFunction {
  private[functions] val rand = new Random(137)

  def toReplayTime(replayStartTime: Long, firstEventTime: Long, eventTime: Long, speedupFactor: Double): Long = {
    val eventTimeSinceStart = eventTime - firstEventTime
    replayStartTime + (eventTimeSinceStart / speedupFactor).toLong
  }

  private case class DelayedWatermark(delayedEventTime: Long)

  private case class DelayedEvent[I](delayedEventTime: Long, event: I)

}
package functions

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.functions.SimpleScaledReplayFunction
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

class SimpleScaledReplayFunctionITSuite extends AbstractTestBase {

  @Test
  def testScaledReplay(): Unit = assertExpectedSpeedup(List(1000L, 2000L, 3000L), 10.0)

  @Test
  def testScaledReplayUnorderedInput(): Unit =
    for (speedupFactor <- List(0.4, 0.9, 1.0, 2.4, 1000.0)) {
      assertExpectedSpeedup(List(1000L, 2000L, 3000L, 2500L, 2700L, 3000L, 3100L), speedupFactor)
    }

  @Test
  def testScaledReplayInfiniteSpeedup(): Unit = assertExpectedSpeedup(List(1000L, 2000L, 3000L), 0.0)

  @Test
  def testScaledReplaySingleElement(): Unit = assertExpectedSpeedup(List(1000L), 1)

  private def assertExpectedSpeedup(eventTimes: Seq[Long], speedupFactor: Double): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val startTime = System.currentTimeMillis

    val stream: DataStream[(Long, Long)] = env.fromCollection(eventTimes)
      .keyBy(_ => 0L)
      .map(new SimpleScaledReplayFunction[Long](identity, speedupFactor))
      .map((_, System.currentTimeMillis)) // output: tuples (event time, emit processing time)

    val replayTimes = DataStreamUtils.collect(stream.javaStream).asScala.toList
    val listWithTimeSinceStart = replayTimes.map(t => (t._1, t._2 - startTime))

    listWithTimeSinceStart.foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = if (speedupFactor == 0) 0 else (eventTimes.max - eventTimes.min) / speedupFactor

    println(s"Speedup factor: $speedupFactor")
    println(s"Duration: $duration")
    println(s"Time differences: ${getDifferenceToPrevious(listWithTimeSinceStart)}")

    // job overhead is ~ 500 ms

    assertResult(eventTimes.length)(listWithTimeSinceStart.length)
    assert(duration >= minDuration)

    assertProcessingTimeWithinTolerance(listWithTimeSinceStart, speedupFactor)
  }

  private def getDifferenceToPrevious(eventAndProcessingTimes: List[(Long, Long)]): List[(Long, Long)] =
    eventAndProcessingTimes
      .drop(1) // start from second element
      .zip(eventAndProcessingTimes) // zip with predecessor
      .map {
      case ((eventTime, procTime), (prevEventTime, prevProcTime)) =>
        (
          eventTime - prevEventTime, // event time difference
          procTime - prevProcTime // processing time difference
        )
    }

  private def assertProcessingTimeWithinTolerance(eventAndProcessingTimes: Seq[(Long, Long)], speedupFactor: Double): Unit = {
    if (eventAndProcessingTimes.nonEmpty) {
      val (firstEventTime, processingStartTime) = eventAndProcessingTimes.head

      var runningMaxEventTime = 0L
      val sinceStart =
        eventAndProcessingTimes
          .drop(1) // start from second element
          .map {
          case (eventTime, procTime) =>
            runningMaxEventTime = math.max(eventTime, runningMaxEventTime)
            (
              runningMaxEventTime, // maximum event time so far
              runningMaxEventTime - firstEventTime,
              eventTime - firstEventTime, // event time difference
              procTime - processingStartTime // processing time difference
            )
        }

      println(s"Time since start: $sinceStart")

      if (speedupFactor > 0) {
        val tolerance = 10L
        sinceStart.foreach {
          case (maxEventTime, maxEventTimeDiff, eventTimeDiff, procTimeDiff) =>
            assert(math.abs(maxEventTimeDiff / speedupFactor - procTimeDiff) <= tolerance,
              s"maxEventTime: $maxEventTime; " +
                s"maxEventTimeDiff: $maxEventTimeDiff; " +
                s"eventTimeDiff: $eventTimeDiff; " +
                s"procTimeDiff: $procTimeDiff")
        }
      }
    }
  }
}

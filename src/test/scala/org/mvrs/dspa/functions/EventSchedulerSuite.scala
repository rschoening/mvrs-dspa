package org.mvrs.dspa.functions

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EventSchedulerSuite extends FlatSpec with Matchers {
  "the event scheduler" must "schedule correctly if no delay" in {
    val scheduler = new EventScheduler[String](100, 15000, 0, _ => 0)

    scheduler.schedule("e1", 10000)
    scheduler.schedule("e2", 20000)
    scheduler.schedule("e3", 30000)

    val schedule = getSchedule(scheduler)

    println(schedule.mkString("\n"))

    assertResult(3)(schedule.count(_.isInstanceOf[ScheduledEvent]))
    assertResult(2)(schedule.count(_.isInstanceOf[ScheduledWatermark]))
    assertWatermarkCoversAllEvents(schedule)
  }

  it must "schedule correctly if with valid maximum delay" in {
    val scheduler = new EventScheduler[String](100, 15000, 10000, _ => 0)

    scheduler.schedule("e1", 10000)
    scheduler.schedule("e2", 20000)
    scheduler.schedule("e3", 30000)

    val schedule = getSchedule(scheduler)

    println(schedule.mkString("\n"))

    assertResult(3)(schedule.count(_.isInstanceOf[ScheduledEvent]))
    assertResult(3)(schedule.count(_.isInstanceOf[ScheduledWatermark]))
    assertWatermarkCoversAllEvents(schedule)
  }

  it must "correctly calculate replay time" in {
    assertResult(1010)(EventScheduler.toReplayTime(replayStartTime = 1000, firstEventTime = 100, eventTime = 200, speedupFactor = 10))
  }

  it must "correctly calculate replay time based on realistic dates" in {
    val hoursLater = 2
    val startTime = System.currentTimeMillis()
    val firstEventTime = LocalDateTime.of(2010, 12, 31, 15, 50, 50)
    val oneHourLater = firstEventTime.plusHours(hoursLater)

    val speedupFactor = 1000
    val expectedDelayMillis = hoursLater * 60 * 60 * 1000 / speedupFactor

    assertResult(startTime + expectedDelayMillis)(EventScheduler.toReplayTime(
      replayStartTime = startTime,
      firstEventTime = firstEventTime.toEpochSecond(ZoneOffset.UTC) * 1000,
      eventTime = oneHourLater.toEpochSecond(ZoneOffset.UTC) * 1000,
      speedupFactor))
  }

  private def assertWatermarkCoversAllEvents(schedule: Seq[ScheduledItem]) = {
    assert(
      schedule.collect { case s: ScheduledEvent => s.timestamp }.max <=
        schedule.collect { case s: ScheduledWatermark => s.watermark.getTimestamp }.max,
      "watermarks must cover all events")
  }

  private def getSchedule(scheduler: EventScheduler[String]): Seq[ScheduledItem] = {
    val result: ArrayBuffer[ScheduledItem] = mutable.ArrayBuffer[ScheduledItem]()

    val start = System.currentTimeMillis()

    scheduler.processPending(
      emitEvent = (event, timestamp) => result += ScheduledEvent(event, timestamp, currentOffset(start)),
      emitWatermark = watermark => result += ScheduledWatermark(watermark, currentOffset(start)),
      wait = waitTime => {
        result += ScheduledWait(waitTime)
        Thread.sleep(waitTime)
      },
      isCancelled = () => false,
      flush = true
    )
    result
  }

  private def currentOffset(start: Long) = {
    System.currentTimeMillis() - start
  }

  trait ScheduledItem

  case class ScheduledEvent(event: String, timestamp: Long, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWatermark(watermark: Watermark, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWait(waitTime: Long) extends ScheduledItem

}

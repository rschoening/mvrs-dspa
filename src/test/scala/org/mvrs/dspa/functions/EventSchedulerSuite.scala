package org.mvrs.dspa.functions

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

    val events = schedule.collect { case s: ScheduledEvent => s }
    val watermarks = schedule.collect { case s: ScheduledWatermark => s }

    assertResult(List("e1", "e2", "e3"))(events.map(_.event))
    assertResult(List(24999, 39999))(watermarks.map(_.watermark.getTimestamp))

    val timeTolerance = 10
    assert(math.abs(events(0).replayTimeOffset - 10) <= timeTolerance) // startup delay (jit, ...)
    assert(math.abs(events(1).replayTimeOffset - 100) <= timeTolerance)
    assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

    assertWatermarkCoversAllEvents(schedule)
  }

  it must "schedule correctly if with valid maximum delay" in {
    val scheduler = new EventScheduler[String](100, 15000, 10000, _ => 0)

    scheduler.schedule("e1", 10000)
    scheduler.schedule("e2", 20000)
    scheduler.schedule("e3", 30000)

    val schedule = getSchedule(scheduler)

    println(schedule.mkString("\n"))

    val events = schedule.collect { case s: ScheduledEvent => s }
    val watermarks = schedule.collect { case s: ScheduledWatermark => s }

    assertResult(List("e1", "e2", "e3"))(events.map(_.event))
    assertResult(List(14999, 29999, 44999))(watermarks.map(_.watermark.getTimestamp))

    val timeTolerance = 10
    assert(math.abs(events(0).replayTimeOffset - 10) <= timeTolerance) // startup delay (jit, ...)
    assert(math.abs(events(1).replayTimeOffset - 100) <= timeTolerance)
    assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

    assertWatermarkCoversAllEvents(schedule)
  }

  it must "schedule correctly if with reordered event times" in {
    val scheduler = new EventScheduler[String](
      speedupFactor = 10,
      watermarkIntervalMillis = 2000,
      maximumDelayMillis = 1300,
      {
        case "e1" => 1300 // delay the first event so that it schedules after the second
        case _ => 0
      })

    scheduler.schedule("e1", 1000)
    scheduler.schedule("e2", 2000)
    scheduler.schedule("e3", 3000)

    val schedule = getSchedule(scheduler)

    println(schedule.mkString("\n"))

    val events = schedule.collect { case s: ScheduledEvent => s }
    val watermarks = schedule.collect { case s: ScheduledWatermark => s }

    assertResult(List("e2", "e1", "e3"))(events.map(_.event))
    assertResult(List(1699, 3699))(watermarks.map(_.watermark.getTimestamp))

    val timeTolerance = 10
    assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance)
    assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance)
    assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

    assertWatermarkCoversAllEvents(schedule)
  }


  it must "schedule correctly if with reordered event times and watermark interval shorter than delay" in {
    val scheduler = new EventScheduler[String](
      speedupFactor = 10,
      watermarkIntervalMillis = 800,
      maximumDelayMillis = 1300,
      {
        case "e1" => 1300 // delay the first event so that it schedules after the second
        case _ => 0
      })

    scheduler.schedule("e1", 1000)
    scheduler.schedule("e2", 2000)
    scheduler.schedule("e3", 3000)

    val schedule = getSchedule(scheduler)

    println(schedule.mkString("\n"))

    val events = schedule.collect { case s: ScheduledEvent => s }
    val watermarks = schedule.collect { case s: ScheduledWatermark => s }

    assertResult(List("e2", "e1", "e3"))(events.map(_.event))
    assertResult(List(499, 1299, 2099, 2899, 3699))(watermarks.map(_.watermark.getTimestamp))

    val timeTolerance = 10
    assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance)
    assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance)
    assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

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

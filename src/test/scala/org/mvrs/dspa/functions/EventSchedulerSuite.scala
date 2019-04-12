package org.mvrs.dspa.functions

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

  "the event scheduler" must "schedule correctly if with valid maximum delay" in {
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

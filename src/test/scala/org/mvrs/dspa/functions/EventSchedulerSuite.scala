package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class EventSchedulerSuite extends FlatSpec with Matchers {
  "the event scheduler" must "schedule correctly if no delay" in {
    val scheduler = new EventScheduler[String](100, 15000, 0, _ => 0)

    scheduler.schedule("e1", 10000)
    scheduler.schedule("e2", 20000)
    scheduler.schedule("e3", 30000)
    // scheduler.schedule("e4", 40000)

    val schedule: mutable.ArrayBuffer[ScheduledItem] = mutable.ArrayBuffer[ScheduledItem]()

    val start = System.currentTimeMillis()

    scheduler.processPending(
      emitEvent = (event, timestamp) => schedule += ScheduledEvent(event, timestamp, currentOffset(start)),
      emitWatermark = watermark => schedule += ScheduledWatermark(watermark, currentOffset(start)),
      wait = waitTime => {
        schedule += ScheduledWait(waitTime)
        Thread.sleep(waitTime)
      },
      isRunning = () => true,
      flush = true
    )

    println(schedule.mkString("\n"))

    // TODO assertions
  }

  private def currentOffset(start: Long) = {
    System.currentTimeMillis() - start
  }

  trait ScheduledItem

  case class ScheduledEvent(event: String, timestamp: Long, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWatermark(watermark: Watermark, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWait(waitTime: Long) extends ScheduledItem

}

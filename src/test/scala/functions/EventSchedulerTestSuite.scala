package functions

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.flink.streaming.api.watermark.Watermark
import org.mvrs.dspa.functions.EventScheduler
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

//noinspection ZeroIndexToHead
class EventSchedulerTestSuite extends FlatSpec with Matchers {
  val timeTolerance = 120 // loose tolerance required when running on battery/busy machine

  "the event scheduler" must "schedule correctly if no delay and no scaling" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")
      val scheduler = new EventScheduler[String](
        speedupFactor = 0,
        watermarkIntervalMillis = Some(15000),
        maximumDelayMillis = 0,
        delay = _ => 0,
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 10000),
          ("e2", 20000),
          ("e3", 30000))
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e1", "e2", "e3"))(events.map(_.event))
      if (minimumWatermarkEmitIntervalMillis == 0) {
        assertResult(List(24999, 39999))(watermarks.map(_.watermark.getTimestamp))
      }

      assert(math.abs(events(0).replayTimeOffset - 10) <= timeTolerance) // there is some startup delay (jit, ...)
      assert(math.abs(events(1).replayTimeOffset - events(0).replayTimeOffset) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - events(1).replayTimeOffset) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(4)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly if no delay" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")
      val scheduler = new EventScheduler[String](
        speedupFactor = 100,
        watermarkIntervalMillis = Some(15000),
        maximumDelayMillis = 0,
        delay = _ => 0,
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 10000),
          ("e2", 20000),
          ("e3", 30000))
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e1", "e2", "e3"))(events.map(_.event))
      assertResult(List(24999, 39999))(watermarks.map(_.watermark.getTimestamp)) // same as for unscaled

      assert(math.abs(events(0).replayTimeOffset - 10) <= timeTolerance) // there is some startup delay (jit, ...)
      assert(math.abs(events(1).replayTimeOffset - 100) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(4)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly with defined maximum delay" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")
      val scheduler = new EventScheduler[String](
        speedupFactor = 100,
        watermarkIntervalMillis = Some(15000),
        maximumDelayMillis = 10000,
        delay = _ => 0,
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 10000),
          ("e2", 20000),
          ("e3", 30000))
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e1", "e2", "e3"))(events.map(_.event))
      assertResult(List(14999, 29999, 44999))(watermarks.map(_.watermark.getTimestamp))

      assert(math.abs(events(0).replayTimeOffset - 10) <= timeTolerance) // startup delay (jit, ...)
      assert(math.abs(events(1).replayTimeOffset - 100) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(4)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly with reordered event times" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")

      val scheduler = new EventScheduler[String](
        speedupFactor = 10,
        watermarkIntervalMillis = Some(2000),
        maximumDelayMillis = 1300,
        {
          case "e1" => 1300 // delay the first event so that it schedules after the second
          case _ => 0
        },
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 1000),
          ("e2", 2000),
          ("e3", 3000))
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e2", "e1", "e3"))(events.map(_.event))
      assertResult(List(1699, 3699))(watermarks.map(_.watermark.getTimestamp))

      assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance)
      assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(4)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly with reordered/scaled event times and LATE events" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")

      val scheduler = new EventScheduler[String](
        speedupFactor = 10,
        watermarkIntervalMillis = Some(2000),
        maximumDelayMillis = 1300,
        {
          case "e2" => 12500 // delay the first event so that it schedules before the last (2000 + 12500 = 14500)
          case _ => 0
        },
        allowLateEvents = true,
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 1000),
          ("e2", 2000),
          ("e3", 3000),
          ("e4", 14000),
          ("e5", 15000),
        )
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e1", "e3", "e4", "e2", "e5"))(events.map(_.event))
      assertResult(List(1699, 3699, 5699, 7699, 9699, 11699, 13699, 15699))(watermarks.map(_.watermark.getTimestamp))

      assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance) // e1
      assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance) // e3
      assert(math.abs(events(2).replayTimeOffset - 1300) <= timeTolerance) // e4
      assert(math.abs(events(3).replayTimeOffset - 1350) <= timeTolerance) // e2
      assert(math.abs(events(4).replayTimeOffset - 1400) <= timeTolerance) // e5

      assertWatermarkCoversAllEvents(schedule)
      if (minimumWatermarkEmitIntervalMillis == 0) {
        // NOTE if > 0, the watermark that results in identifying this event as late may be skipped
        assertResult(Set("e2"))(getLateEvents(schedule))
      }
      assertResult(6)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly with reordered/UNscaled event times and LATE events" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")

      val scheduler = new EventScheduler[String](
        speedupFactor = 0,
        watermarkIntervalMillis = Some(2000),
        maximumDelayMillis = 1300,
        {
          case "e2" => 12500 // delay the first event so that it schedules before the last (2000 + 12500 = 14500)
          case _ => 0
        },
        allowLateEvents = true,
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 1000),
          ("e2", 2000),
          ("e3", 3000),
          ("e4", 14000),
          ("e5", 15000),
        )
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e1", "e3", "e4", "e2", "e5"))(events.map(_.event))

      events.foreach(e => assert(e.replayTimeOffset <= timeTolerance, e))

      assertWatermarkCoversAllEvents(schedule)
      if (minimumWatermarkEmitIntervalMillis == 0) {
        // NOTE if > 0, the watermark that results in identifying this event as late may be skipped
        assertResult(Set("e2"))(getLateEvents(schedule))
        assertResult(List(1699, 3699, 5699, 7699, 9699, 11699, 13699, 15699))(watermarks.map(_.watermark.getTimestamp))
      }
      assertResult(6)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly with reordered event times, incrementally" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")

      val scheduler = new EventScheduler[String](
        speedupFactor = 10,
        watermarkIntervalMillis = Some(2000),
        maximumDelayMillis = 1300,
        {
          case "e1" => 1300 // delay the first event so that it schedules after the second
          case _ => 0
        },
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = new mutable.ArrayBuffer[ScheduledItem]()
      val startTime = System.currentTimeMillis()

      scheduler.schedule("e1", 1000)
      schedule ++= getSchedule(scheduler, flush = false, startTime)

      scheduler.schedule("e2", 2000)
      schedule ++= getSchedule(scheduler, flush = false, startTime)

      scheduler.schedule("e3", 3000)
      schedule ++= getSchedule(scheduler, flush = true, startTime)

      println(schedule.mkString("\n"))

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e2", "e1", "e3"))(events.map(_.event))
      assertResult(List(1699, 3699))(watermarks.map(_.watermark.getTimestamp))

      assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance)
      assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(3)(scheduler.maximumScheduleLength)
    }
  }

  it must "schedule correctly if with reordered event times and watermark interval shorter than delay" in {
    for (minimumWatermarkEmitIntervalMillis <- List(0, 10, 100)) {
      println(s"\nMinimum watermark emit interval: $minimumWatermarkEmitIntervalMillis")

      val scheduler = new EventScheduler[String](
        speedupFactor = 10,
        watermarkIntervalMillis = Some(800),
        maximumDelayMillis = 1300,
        {
          case "e1" => 1300 // delay the first event so that it schedules after the second
          case _ => 0
        },
        minimumWatermarkEmitIntervalMillis = minimumWatermarkEmitIntervalMillis)

      val schedule = scheduleEvents(
        scheduler,
        List(
          ("e1", 1000),
          ("e2", 2000),
          ("e3", 3000))
      )

      val events = schedule.collect { case s: ScheduledEvent => s }
      val watermarks = schedule.collect { case s: ScheduledWatermark => s }

      assertResult(List("e2", "e1", "e3"))(events.map(_.event))
      if (minimumWatermarkEmitIntervalMillis == 0) {
        assertResult(List(499, 1299, 2099, 2899, 3699))(watermarks.map(_.watermark.getTimestamp))
      }

      assert(math.abs(events(0).replayTimeOffset - 100) <= timeTolerance)
      assert(math.abs(events(1).replayTimeOffset - 130) <= timeTolerance)
      assert(math.abs(events(2).replayTimeOffset - 200) <= timeTolerance)

      assertWatermarkCoversAllEvents(schedule)
      assertNoLateEvents(schedule)
      assertResult(4)(scheduler.maximumScheduleLength)
    }
  }

  it must "correctly calculate replay time" in {
    assertResult(1010)(EventScheduler.toReplayTime(
      replayStartTime = 1000, firstEventTime = 100, eventTime = 200, speedupFactor = 10))
  }

  it must "correctly calculate replay time for an example of realistic dates" in {
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

  private def assertNoLateEvents(schedule: Seq[ScheduledItem], exceptions: Set[String] = Set()): Unit = {
    assertResult(Set())(getLateEvents(schedule))
  }

  private def getLateEvents(schedule: Seq[ScheduledItem]): Set[String] =
    schedule.foldLeft( // initial value:
      (
        Set[String](), // set of late events
        Long.MinValue // the maximum watermark value seen so far
      )
    ) {
      case ((late, maxWm), ScheduledWatermark(wm, _)) => (late, math.max(maxWm, wm.getTimestamp))
      case ((late, maxWm), ScheduledEvent(e, timestamp, _)) => (if (timestamp < maxWm) late + e else late, maxWm)
      case (acc, _) => acc // just pass it on
    }._1

  private def scheduleEvents(scheduler: EventScheduler[String], events: Iterable[(String, Long)]): Seq[ScheduledItem] = {
    println(s"speedup factor: ${scheduler.speedupFactor}")
    println(s"watermark interval (ms): ${scheduler.watermarkIntervalMillis}")
    println(s"maximum delay (ms): ${scheduler.maximumDelayMillis}")
    println()
    println("Input events:")
    events.foreach(e => println(s"- $e"))

    scheduler.schedule(events)

    val schedule = getSchedule(scheduler)

    println()
    println("=> schedule:")
    schedule.foreach(e => println(s"- $e"))

    schedule
  }

  private def getSchedule(scheduler: EventScheduler[String],
                          flush: Boolean = true,
                          startTime: Long = System.currentTimeMillis()): Seq[ScheduledItem] = {
    val result = mutable.ArrayBuffer[ScheduledItem]()

    scheduler.processPending(
      emitEvent = (event, timestamp) => result += ScheduledEvent(event, timestamp, currentOffset(startTime)),
      emitWatermark = watermark => result += ScheduledWatermark(watermark, currentOffset(startTime)),
      wait = waitTime => {
        result += ScheduledWait(waitTime)
        Thread.sleep(waitTime)
      },
      isCancelled = () => false,
      flush
    )
    result
  }

  private def currentOffset(start: Long) = System.currentTimeMillis() - start

  sealed trait ScheduledItem

  case class ScheduledEvent(event: String, timestamp: Long, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWatermark(watermark: Watermark, replayTimeOffset: Long) extends ScheduledItem

  case class ScheduledWait(waitTime: Long) extends ScheduledItem

}

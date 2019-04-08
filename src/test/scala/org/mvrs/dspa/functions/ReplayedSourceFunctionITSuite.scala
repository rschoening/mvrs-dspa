package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

class ReplayedSourceFunctionITSuite extends AbstractTestBase {

  @Test
  def testScaledReplay(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val startTime = System.currentTimeMillis

    // create a stream of custom elements and apply transformations
    val eventTimes = List(1000L, 2000L, 3000L)

    val stream: DataStream[(Long, Long)] =
      env
        .addSource(new ReplayedSequenceSourceFunction[Long](eventTimes, identity[Long], 1, 0))
        .map((_, System.currentTimeMillis))

    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList
    list.map(t => (t._1, t._2 - startTime)).foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    println(s"Duration: $duration")

    // job overhead is ~ 500 ms

    // TODO come up with tighter assertion (based on processing times in collected tuples)
    assertResult(eventTimes.length)(list.length)
    assert(duration >= minDuration)
  }

  @Test
  def testScaledAndDelayedReplay(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)

    // create a stream of custom elements and apply transformations
    val eventTimes = List(1000L, 2000L, 3000L)

    val startTime = System.currentTimeMillis()

    val stream: DataStream[(Long, Long)] =
      env.addSource(new ReplayedSequenceSourceFunction[Long](eventTimes, identity, 1, 2200, {
        case 1000L => 2200
        case 2000L => 1300
        case 3000L => 100
      }))
        .map((_, System.currentTimeMillis))

    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList
    list.map(t => (t._1, t._2 - startTime)).foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    println(s"Duration: $duration")

    // job overhead is ~ 500 ms

    // TODO come up with tighter assertion (based on processing times in collected tuples)
    assertResult(eventTimes.length)(list.length)
    assert(duration >= minDuration)
  }

  @Test
  def testScaledReplaySingleEvent(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // create a stream of custom elements and apply transformations
    val eventTimes = List(1000L)

    val startTime = System.currentTimeMillis()

    val stream: DataStream[(Long, Long)] =
      env
        .addSource(new ReplayedSequenceSourceFunction[Long](eventTimes, identity, 1, 100))
        .map((_, System.currentTimeMillis))

    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList

    list.map(t => (t._1, t._2 - startTime)).foreach(println(_))

    assertResult(eventTimes.length)(list.length)
  }

}

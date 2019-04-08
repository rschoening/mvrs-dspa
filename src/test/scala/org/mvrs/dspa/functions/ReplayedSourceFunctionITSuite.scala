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
    val eventTimes = List(10000L, 20000L, 30000L)
    val speedupFactor = 10.0

    val stream: DataStream[(Long, Long)] =
      env
        .addSource(new ReplayedSequenceSourceFunction[Long](eventTimes, identity[Long], speedupFactor, 0))
        .map((_, System.currentTimeMillis))

    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList
    list.map(t => (t._1, t._2 - startTime)).foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    // job overhead is ~ 500 ms

    assertResult(eventTimes.length)(list.length)
    assertResult(List(10000, 20000, 30000))(list.map(_._1))
    assert(duration >= minDuration / speedupFactor)
    assert(expectedDelays(list, speedupFactor))
  }

  def expectedDelays(results: List[(Long, Long)], speedupFactor: Double, tolerance: Double = 10): Boolean = {
    results.zipWithIndex.drop(1).forall {
      case ((eventTime, procTime), index) =>
        val previous = results(index - 1)
        val previousEventTime = previous._1
        val previousProcTime = previous._2

        math.abs((procTime - previousProcTime) - ((eventTime - previousEventTime) / speedupFactor)) <= tolerance
    }
  }

  @Test
  def testScaledAndDelayedReplay(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)

    // create a stream of custom elements and apply transformations
    val eventTimes = List(0L, 1000L, 2000L)

    val startTime = System.currentTimeMillis()

    val stream: DataStream[(Long, Long)] =
      env.addSource(new ReplayedSequenceSourceFunction[Long](eventTimes, identity, 1, 2200, {
        case 0L => 2200 // expected second (~ 2800 ms after first event, including ~600 ms job overhead)
        case 1000L => 1300 // expected third  (~ 2900 ms)
        case 2000L => 100 // expected first  (~ 2700 ms)
      }, watermarkInterval = 1000))
        .map((_, System.currentTimeMillis))

    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList
    list.map(t => (t._1, t._2 - startTime)).foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    // job overhead is ~ 500 ms

    assertResult(eventTimes.length)(list.length)
    assertResult(List(2000, 0, 1000))(list.map(_._1))
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

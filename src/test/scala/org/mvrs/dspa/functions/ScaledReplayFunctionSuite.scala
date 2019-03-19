package org.mvrs.dspa.functions

import java.util.Calendar

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test

class ScaledReplayFunctionSuite extends AbstractTestBase {

  @Test
  def testScaledReplay(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)

    val startTime = System.currentTimeMillis

    // create a stream of custom elements and apply transformations
    val eventTimes = List(1000L, 2000L, 1000L)

    env.fromCollection(eventTimes)
      .process(new ScaledReplayFunction(identity, 1, 100))
      .map((_, Calendar.getInstance.getTime.toInstant))
      .print
    env.execute()

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    println(s"Duration: $duration")

    // job overhead is ~ 500 ms

    // TODO come up with tighter assertion
    assert(duration >= minDuration)
  }
}


//     val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream = env.fromElements(10000, 20000, 30000)
//      .process(new ScaledReplayFunction(e => e.toLong, 100.0, 10))
//
//    stream.print()
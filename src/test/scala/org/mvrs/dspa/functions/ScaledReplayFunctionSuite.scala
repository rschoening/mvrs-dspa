package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test

import scala.collection.JavaConverters._

class ScaledReplayFunctionSuite extends AbstractTestBase {

  @Test
  def testScaledReplay(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(1)

    val startTime = System.currentTimeMillis

    // create a stream of custom elements and apply transformations
    val eventTimes = List(1000L, 2000L, 1000L)

    val stream: DataStream[(Long, Long)] = env.fromCollection(eventTimes)
      .process(new ScaledReplayFunction(identity, 1, 100))
      .map((_, System.currentTimeMillis))

    env.execute()

    // Note: this must be called *after* execute()
    val list = DataStreamUtils.collect(stream.javaStream).asScala.toList
    list.foreach(println(_))

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime
    val minDuration = eventTimes.max - eventTimes.min

    println(s"Duration: $duration")

    // job overhead is ~ 500 ms

    // TODO come up with tighter assertion (based on processing times in collected tuples)
    assert(duration >= minDuration)
  }
}

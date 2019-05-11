package streams

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.streams
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.Assertions._
import utils.TestUtils

import scala.collection.JavaConverters._

class InputStreamsITSuite extends AbstractTestBase {
  @Test
  def testReadingLikes(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    val startTime = System.currentTimeMillis()

    streams
      .likesFromCsv(TestUtils.getResourceURIPath("/streams/likes.csv"))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    print(results)

    assertResult(117151)(results.map(_._2).sum) // like event count
    assertResult(26623)(results.map(_._1).distinct.size) // distinct posts
    assertResult(29741)(results.size) // count of per-post windows


    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.3 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingPosts(): Unit = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .postsFromCsv(TestUtils.getResourceURIPath("/streams/posts.csv"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    print(results)

    assertResult(30956)(results.map(_._2).sum) // post event count
    assertResult(382)(results.map(_._1).distinct.size) // distinct persons
    assertResult(1266)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")

    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.2 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingRawComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .rawCommentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    print(results)

    assertResult(113422)(results.map(_._2).sum) // post event count
    assertResult(822)(results.map(_._1).distinct.size) // distinct persons
    assertResult(3880)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")

    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.2 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    val startTime = System.currentTimeMillis()

    streams
      .commentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    print(results)

    // possible causes for non-determinism
    // - parallelism > 1 (no, at least event count is different also for p=1)
    // - some processing time-dependency

    // assertResult(xxx)(results.map(_._2).sum) // post event count NOTE currently not deterministic
    assertResult(11575)(results.map(_._1).distinct.size) // distinct posts
    // assertResult(xxx)(results.size) // count of per-post windows NOTE currently not deterministic

    // Before changes to ensure deterministic outputs:
    // - parallelism=1: 3.5 seconds
    // - parallelism=4: 15 seconds !!! due to broadcasting in reply tree reconstruction

    // After changes:
    // - parallelism=1: XX seconds
    // - parallelism=4: XX seconds
    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }

  private def print(results: List[(Long, Int)]): Unit = {
    println(results.map(_._2).sum) // like event count
    println(results.map(_._1).distinct.size) // distinct posts
    println(results.size) // count of per-post windows
  }
}

class CounterSink extends SinkFunction[(Long, Int)] {
  override def invoke(value: (Long, Int)): Unit = CounterSink.values.add(value)
}

object CounterSink {
  // NOTE using
  // synchronized { /* access to non-threadsafe collection */ }
  // does not work, collection still corrupt --> Flink documentation should be changed
  val values: util.Collection[(Long, Int)] = Collections.synchronizedCollection(new util.ArrayList[(Long, Int)])
}
package streams

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

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
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

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
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

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
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

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
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .commentsFromCsv(
        TestUtils.getResourceURIPath("/streams/comments.csv"),
        10000000,
        lookupParentPostId = replies => replies.map(Left(_)),
        postMappingTtl = None
      )._1
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

    print(results)

    assertResult(107142)(results.map(_._2).sum) // comment event count
    assertResult(11575)(results.map(_._1).distinct.size) // distinct posts
    assertResult(12559)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }

  @Test
  def testReadingCommentsNonWindowed(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .commentsFromCsv(
        TestUtils.getResourceURIPath("/streams/comments.csv"),
        lookupParentPostId = replies => replies.map(Left(_)),
        postMappingTtl = None
      )._1
      .map(e => (e.postId, 1))
      .addSink(new CounterSink[(Long, Int)])

    CounterSink.counter.set(0)

    env.execute()

    val result = CounterSink.counter.get()

    println(result)

    assertResult(107142)(result)

    println(s"duration: ${DateTimeUtils.formatDuration(System.currentTimeMillis() - startTime)}")
  }

  @Test
  def testCompareComments(): Unit = {
    val result1 = getCommentIds
    val result2 = getCommentIds

    println(s"result size 1: ${result1.size} - 2: ${result2.size}")

    assertResult(Set())(result1 -- result2)
    assertResult(Set())(result2 -- result1)
  }

  private def getCommentIds: Set[Long] = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.setParallelism(1)

    CollectionSink.values.clear()
    streams
      .commentsFromCsv(
        TestUtils.getResourceURIPath("/streams/comments.csv"), watermarkInterval = 100,
        lookupParentPostId = replies => replies.map(Left(_)),
        postMappingTtl = None
      )._1
      .map(e => (e.commentId, 1))
      .addSink(new CollectionSink())

    env.execute()

    CollectionSink.values.asScala.map(_._1).toSet
  }

  private def print(results: List[(Long, Int)]): Unit = {
    println(results.map(_._2).sum) // like event count
    println(results.map(_._1).distinct.size) // distinct posts
    println(results.size) // count of per-post windows
  }
}

class CounterSink[T] extends SinkFunction[T] {
  override def invoke(value: T): Unit = CounterSink.counter.incrementAndGet()
}

object CounterSink {
  val counter: AtomicLong = new AtomicLong(0)
}

class CollectionSink extends SinkFunction[(Long, Int)] {
  override def invoke(value: (Long, Int)): Unit = CollectionSink.values.add(value)
}

object CollectionSink {
  // NOTE using
  // synchronized { /* access to non-threadsafe collection */ }
  // does not work, collection still corrupt --> Flink documentation should be changed
  val values: util.Collection[(Long, Int)] = Collections.synchronizedCollection(new util.ArrayList[(Long, Int)])
}
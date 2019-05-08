package streams

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.{Ignore, Test}
import org.mvrs.dspa.utils.DateTimeUtils
import org.mvrs.dspa.{Settings, streams}
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

// TODO add small-enough (for git) subset of test data to resources

class InputStreamsITSuite extends AbstractTestBase {
  @Ignore("depends on configured test data, counts apply for small dataset")
  @Test
  def testReadingLikes(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    streams
      .likesFromCsv(Settings.config.getString("data.likes-csv-path"))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    assertResult(662890)(results.map(_._2).sum) // like event count
    assertResult(150538)(results.map(_._1).distinct.size) // distinct posts
    assertResult(167229)(results.size) // count of per-post windows
  }

  @Ignore("depends on configured test data, counts apply for small dataset")
  @Test
  def testReadingPosts(): Unit = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    val startTime = System.currentTimeMillis()

    streams
      .postsFromCsv(Settings.config.getString("data.posts-csv-path"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    assertResult(173401)(results.map(_._2).sum) // post event count
    assertResult(984)(results.map(_._1).distinct.size) // distinct persons
    assertResult(6381)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }

  @Ignore("depends on configured test data, counts apply for small dataset")
  @Test
  def testReadingRawComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    val startTime = System.currentTimeMillis()

    streams
      .rawCommentsFromCsv(Settings.config.getString("data.comments-csv-path"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    assertResult(632042)(results.map(_._2).sum) // post event count
    assertResult(912)(results.map(_._1).distinct.size) // distinct persons
    assertResult(8966)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }

  @Ignore("depends on configured test data, counts apply for small dataset")
  @Test
  def testReadingComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams.commentsFromCsv(
      Settings.config.getString("data.comments-csv-path"))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    val eventCount = results.map(_._2).sum
    val distinctPostCount = results.map(_._1).distinct.size
    val windowCount = results.size

    println(s"event count: $eventCount") // 597785, 597796, 597788 for parallelism = 1
    println(s"distinct posts: $distinctPostCount")
    println(s"per-post windows: $windowCount") // 68492  - reproduced with parallelism = 1

    // possible causes for non-determinism
    // - parallelism > 1 (no, at least event count is different also for p=1)
    // - some processing time-dependency

//    assertResult(604529)(eventCount) // post event count NOTE currently not deterministic
    assertResult(63177)(distinctPostCount) // distinct posts
    // assertResult(68499)(windowCount) // count of per-post windows NOTE currently not deterministic

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }
}

class CounterSink extends SinkFunction[(Long, Int)] {
  override def invoke(value: (Long, Int)): Unit = CounterSink.values.add(value)
}

object CounterSink {
  // NOTE using
  // synchronized { /* access to non-threadsafe collection */ }
  // does not work, collection still corrupt
  val values: util.Collection[(Long, Int)] = Collections.synchronizedCollection(new util.ArrayList[(Long, Int)])
}
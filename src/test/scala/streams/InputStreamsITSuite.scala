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
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

class InputStreamsITSuite extends AbstractTestBase {
  @Test
  def testReadingLikes(): Unit = {
    // TODO get from configuration
    val filePath = "C:\\data\\dspa\\project\\1k-users-sorted\\streams\\likes_event_stream.csv"

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(1000L) // required for watermarks and correct timers
    env.setParallelism(4)

    streams
      .likesFromCsv(filePath)
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

  @Test
  def testReadingPosts(): Unit = {
    // TODO get from configuration
    val filePath = "C:\\data\\dspa\\project\\1k-users-sorted\\streams\\post_event_stream.csv"

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(1000L) // required for watermarks and correct timers
    env.setParallelism(4)

    streams
      .postsFromCsv(filePath)
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
  }

  @Test
  def testReadingRawComments(): Unit = {
    // TODO get from configuration
    val filePath = "C:\\data\\dspa\\project\\1k-users-sorted\\streams\\comment_event_stream.csv"

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(1000L) // required for watermarks and correct timers
    env.setParallelism(4)

    streams
      .rawCommentsFromCsv(filePath)
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
  }

  @Test
  def testReadingComments(): Unit = {
    // TODO get from configuration
    val filePath = "C:\\data\\dspa\\project\\1k-users-sorted\\streams\\comment_event_stream.csv"

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(1000L) // required for watermarks and correct timers
    env.setParallelism(4)

    streams.resolveReplyTree(streams.rawCommentsFromCsv(filePath))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CounterSink)

    CounterSink.values.clear()

    env.execute()

    val results = CounterSink.values.asScala.toList

    //    assertResult(602120)(results.map(_._2).sum) // post event count NOTE currently not deterministic
    assertResult(63177)(results.map(_._1).distinct.size) // distinct posts
    //    assertResult(68499)(results.size) // count of per-post windows NOTE currently not deterministic
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
package preparation

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.events.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.{streams, utils}
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters._

/**
  * Integration test suite for [[org.mvrs.dspa.preparation.BuildReplyTreeProcessFunction]]
  *
  * since the execution is not deterministic with parallelism > 1, the tests are repeated
  */
class BuildReplyTreeProcessFunctionITSuite extends AbstractTestBase {

  private val repetitions = 20

  @Test
  def testOrderedInput(): Unit = for (_ <- 0 until repetitions) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(10L) // required for watermarks and correct timers
    env.setParallelism(4)

    val postId = 999
    val personId = 1
    val rawComments: List[RawCommentEvent] = List(
      RawCommentEvent(commentId = 111, personId, creationDate = utils.toDate(1000), None, None, None, Some(postId), None, 0),
      RawCommentEvent(commentId = 112, personId, creationDate = utils.toDate(2000), None, None, None, None, Some(111), 0),
      RawCommentEvent(commentId = 113, personId, creationDate = utils.toDate(3000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 114, personId, creationDate = utils.toDate(4000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 115, personId, creationDate = utils.toDate(5000), None, None, None, None, Some(113), 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[RawCommentEvent](Time.milliseconds(100), _.timestamp))

    val (rootedStream, droppedStream) = streams.resolveReplyTree(stream, droppedRepliesStream = true)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values.asScala
    val dropped = DroppedSink.values.asScala

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

    assertResult(0)(dropped.size)
    assertResult(rawComments.length)(rooted.size)
    assert(rooted.forall(_.postId == postId))
  }

  @Test
  def testUnorderedInput(): Unit = for (_ <- 0 until repetitions) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(10L) // required for watermarks and correct timers
    env.setParallelism(4)

    val postId = 999
    val personId = 1
    val rawComments: List[RawCommentEvent] = List(
      RawCommentEvent(commentId = 114, personId, creationDate = utils.toDate(1000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 115, personId, creationDate = utils.toDate(1000), None, None, None, None, Some(113), 0),
      RawCommentEvent(commentId = 112, personId, creationDate = utils.toDate(3000), None, None, None, None, Some(111), 0),
      RawCommentEvent(commentId = 113, personId, creationDate = utils.toDate(4000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 111, personId, creationDate = utils.toDate(5000), None, None, None, Some(postId), None, 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[RawCommentEvent](Time.milliseconds(100), _.timestamp))

    val (rootedStream, droppedStream) = streams.resolveReplyTree(stream, droppedRepliesStream = true)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values.asScala
    val dropped = DroppedSink.values.asScala

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

    assertResult(rawComments.length)(rooted.size)
    assert(rooted.forall(_.postId == postId))
  }


  @Test
  def dropDanglingReplies(): Unit = for (_ <- 0 until repetitions) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(10L) // required for watermarks and correct timers
    env.setParallelism(1)

    // NOTE it seems that events are broadcasted *more* than once per worker, at least in this test context
    //      (maybe an influence of collect() method below?)
    val postId = 999
    val personId = 1
    val rawComments: List[RawCommentEvent] = List(
      RawCommentEvent(commentId = 114, personId, creationDate = utils.toDate(1000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 115, personId, creationDate = utils.toDate(1000), None, None, None, None, Some(113), 0), // child of dangling parent
      RawCommentEvent(commentId = 112, personId, creationDate = utils.toDate(3000), None, None, None, None, Some(111), 0),
      RawCommentEvent(commentId = 113, personId, creationDate = utils.toDate(4000), None, None, None, None, Some(888), 0), // dangling, unknown parent
      RawCommentEvent(commentId = 111, personId, creationDate = utils.toDate(5000), None, None, None, Some(postId), None, 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[RawCommentEvent](Time.milliseconds(100), _.timestamp))

    val (rootedStream, droppedStream) = streams.resolveReplyTree(stream, droppedRepliesStream = true)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values.asScala
    val dropped = DroppedSink.values.asScala

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

    assertResult(3)(rooted.size)
    val danglingIds = List(113, 115)
    assertResult(2)(dropped.size)
    assert(dropped.forall(_.replyToPostId.isEmpty))
    assert(dropped.forall(r => danglingIds.contains(r.commentId)))
  }
}

// sinks: see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html
// NOTE: these classes must not be nested in the test class, otherwise they are not serializable

class RootedSink extends SinkFunction[CommentEvent] {
  override def invoke(value: CommentEvent): Unit = RootedSink.values.add(value)
}

object RootedSink {
  val values: util.Collection[CommentEvent] = Collections.synchronizedCollection(new util.ArrayList[CommentEvent])
}

class DroppedSink extends SinkFunction[RawCommentEvent] {
  override def invoke(value: RawCommentEvent): Unit = DroppedSink.values.add(value)
}

object DroppedSink {
  // NOTE synchronized { /* access to non-threadsafe collection */ } does not work, collection still corrupt
  val values: util.Collection[RawCommentEvent] = Collections.synchronizedCollection(new util.ArrayList[RawCommentEvent])
}

package jobs.preparation

import java.util
import java.util.Collections

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.model.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters._

/**
  * Integration test suite for [[BuildReplyTreeProcessFunction]]
  *
  */
class BuildReplyTreeProcessFunctionITSuite extends AbstractTestBase {

  private val repetitions = 20
  private val postId = 999

  @Test
  def test_Grandchild_Child_Parent(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createReplyTo(commentId = 113, 1000, 112),
        createReplyTo(commentId = 112, 3000, 111),
        createComment(commentId = 111, 5000, postId),
      )
    )

    // execution:
    // 1. primary 111
    // 2. broadcast 113 (grandchild) --> NOT REPORTED
    // 3. broadcast 112 (child)
    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111))(rooted.map(_.commentId).toSet)
    assertResult(Set(112, 113))(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_Grandchild_Parent_Child(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createReplyTo(commentId = 113, 1000, 112),
        createComment(commentId = 111, 2000, postId),
        createReplyTo(commentId = 112, 3000, 111),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111, 112))(rooted.map(_.commentId).toSet)
    assertResult(Set(113))(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_Parent_Child_Grandchild(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createComment(commentId = 111, 1000, postId),
        createReplyTo(commentId = 112, 2000, 111),
        createReplyTo(commentId = 113, 3000, 112),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111, 112, 113))(rooted.map(_.commentId).toSet)
    assertResult(Set())(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_Parent_Grandchild_Child(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createComment(commentId = 111, 1000, postId),
        createReplyTo(commentId = 113, 2000, 112),
        createReplyTo(commentId = 112, 3000, 111),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111, 112))(rooted.map(_.commentId).toSet)
    assertResult(Set(113))(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_Child_Parent_Grandchild(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      List(
        createReplyTo(commentId = 112, 1000, 111),
        createComment(commentId = 111, 2000, postId),
        createReplyTo(commentId = 113, 3000, 112)
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111))(rooted.map(_.commentId).toSet)
    assertResult(Set(112, 113))(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_Child_Grandchild_Parent(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      List(
        createReplyTo(commentId = 112, 1000, 111),
        createReplyTo(commentId = 113, 3000, 112),
        createComment(commentId = 111, 2000, postId),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111))(rooted.map(_.commentId).toSet)
    assertResult(Set(112, 113))(dropped.map(_.commentId).toSet)
  }

  @Test
  def testOrderedInput(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createComment(commentId = 111, 1000, postId),
        createReplyTo(commentId = 112, 2000, 111),
        createReplyTo(commentId = 113, 3000, 112),
        createReplyTo(commentId = 114, 4000, 112),
        createReplyTo(commentId = 115, 5000, 113),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(0)(dropped.size)
    assertResult(Set(111, 112, 113, 114, 115))(rooted.map(_.commentId).toSet)
  }

  @Test
  def dropDirectChildrenOfLateComments(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      List(
        createReplyTo(commentId = 112, 2000, 111), // direct child --> DROP
        createReplyTo(commentId = 113, 3000, 111), // direct child --> DROP
        createComment(commentId = 111, 9999, postId), // first level --> EMIT
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111))(rooted.map(_.commentId).toSet)
    assertResult(Set(112, 113))(dropped.map(_.commentId).toSet)
  }

  private def buildReplyTree(rawComments: Seq[RawCommentEvent]) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(10L) // required for watermarks and correct timers
    env.setParallelism(1) // necessary to make sense of stream of dropped elements

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[RawCommentEvent](Time.milliseconds(100), _.timestamp))

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

    assert(dropped.forall(_.replyToPostId.isEmpty))

    (rooted, dropped)
  }

  def createComment(commentId: Long, timestamp: Long, postId: Long, personId: Long = 1): RawCommentEvent =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, Some(postId), None, 0)

  def createReplyTo(commentId: Long, timestamp: Long, repliedToCommentId: Long, personId: Long = 1) =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, None, Some(repliedToCommentId), 0)
}

// sinks: see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html
// NOTE: these classes must not be nested in the test class, otherwise they are not serializable

class RootedSink extends SinkFunction[CommentEvent] {
  override def invoke(value: CommentEvent): Unit = RootedSink.values.add(value)
}

object RootedSink {
  // NOTE synchronized { /* access to non-threadsafe collection */ } does not work, collection still corrupt
  val values: util.Collection[CommentEvent] = Collections.synchronizedCollection(new util.ArrayList[CommentEvent])
}

class DroppedSink extends SinkFunction[RawCommentEvent] {
  override def invoke(value: RawCommentEvent): Unit = DroppedSink.values.add(value)
}

object DroppedSink {
  // NOTE synchronized { /* access to non-threadsafe collection */ } does not work, collection still corrupt
  val values: util.Collection[RawCommentEvent] = Collections.synchronizedCollection(new util.ArrayList[RawCommentEvent])
}

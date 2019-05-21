package streams

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

  /*
      ------------------------------------------------------------
      | id     | uid | creation date        | post   | parent    |
      ------------------------------------------------------------
      | 875890 |     | 2012-02-27T00:24:04Z |        | 875870  v | reference to later - must ALWAYS be discarded - and apparently IS
      | 875870 |     | 2012-02-27T00:44:50Z | 200600 |           |
   ** | 875910 |     | 2012-02-27T08:28:22Z |        | 875890  ^ | reference to discarded reply - sometimes not discarded
   ** | 875930 |     | 2012-02-28T00:31:07Z |        | 875910  ^ |
   ** | 875970 |     | 2012-02-29T01:23:17Z |        | 875930  ^ |
   ** | 876010 |     | 2012-03-01T00:28:28Z |        | 875970  ^ | 876010 is leaf
      ------------------------------------------------------------

      Set(875890, 875870, 875910, 875930, 875970, 876010)

observed executions:

processBroadcastElement(875890)
processElement(875870)
processBroadcastElement(875910)
processBroadcastElement(875930)
processBroadcastElement(875970)
processBroadcastElement(876010)

processBroadcastElement(875890)
processBroadcastElement(875910)
processBroadcastElement(875930)
processElement(875870)
processBroadcastElement(875970)
processBroadcastElement(876010)

Differences:
result size 1: 107163 - 2: 107164
1 - 2: Set(3429410, 1366870, 1366890, 3429340, 1885810, 5034250, 1157800)
2 - 1: Set(875970, 875930, 876010, 875910, 1045560, 3571870, 3571880, 3571900)
   */

  @Test
  def test_observedCase(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createReplyTo(875890, 1000, 875870),
        createComment(commentId = 875870, 1010, postId), // parent
        createReplyTo(commentId = 875910, 2000, 875890),
        createReplyTo(commentId = 875930, 3000, 875910),
        createReplyTo(commentId = 875970, 4000, 875930),
        createReplyTo(commentId = 876010, 5000, 875970),
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(875870))(rooted.map(_.commentId).toSet)
    assertResult(Set(875970, 875930, 876010, 875910, 875890))(dropped.map(_.commentId).toSet)
  }

  @Test
  def test_observedCase_alternateExecution(): Unit = for (_ <- 0 until repetitions) {

    // NOTE in the unit test, the first-level comment is always delivered first. The issue cannot be reproduced.
    // NOTE there seems to be no difference for these events if the execution order for these elements is equal.

    // TODO code review: how can different execution order (first level comment received much later) still affect results?

    val (rooted, dropped) = buildReplyTree(
      Seq(
        createReplyTo(875890, 1000, 875870), // always dropped
        createReplyTo(commentId = 875910, 2000, 875890), // sometimes emitted, must be dropped!
        createReplyTo(commentId = 875930, 3000, 875910), // sometimes emitted, must be dropped!
        createComment(commentId = 875870, 1010, postId), // parent --> emitted
        createReplyTo(commentId = 875970, 4000, 875930), // sometimes emitted, must be dropped!
        createReplyTo(commentId = 876010, 5000, 875970), // sometimes emitted, must be dropped!
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(875870))(rooted.map(_.commentId).toSet)
    assertResult(Set(875970, 875930, 876010, 875910, 875890))(dropped.map(_.commentId).toSet)
  }

  /*
   -> | 236570 | 825 | 2012-03-18T04:11:43Z | 54520 |    -    | (1)  parent -> post   (line 9385)
   -> | 236620 | 109 | 2012-03-18T04:56:00Z |   -   | 236590  | (2)  grandchild       (line 9412)
   -> | 236590 | 642 | 2012-03-18T07:38:42Z |   -   | 236570  | (3)  child            (line 9494, + 2h 42 minutes)
   -> | 236640 | 956 | 2012-03-18T10:30:44Z |   -   | 236590  | (4)  grandchild       (line 9622)
   ** | 236650 | 102 | 2012-03-19T02:14:21Z |   -   | 236620  | (5)  great-grandchild (line 9749)

   children of 236590: 236640, 236620
   */
  @Test
  def test_Parent_Grandchild_Child_Greatgrandchild(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createComment(commentId = 236570, 1000, postId), // parent
        createReplyTo(commentId = 236620, 2000, 236590), // grandchild
        createReplyTo(commentId = 236590, 3000, 236570), // child
        createReplyTo(commentId = 236640, 4000, 236590), // grandchild
        createReplyTo(commentId = 236650, 5000, 236620), // great-grandchild (dropped but not reported)
      )
    )

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(236590, 236570, 236640))(rooted.map(_.commentId).toSet)
    assertResult(Set(236620, 236650))(dropped.map(_.commentId).toSet) // got Set(236620) -> great-grandchild not reported
  }

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
  def test_Parent_Grandchild_Child_unordered(): Unit = for (_ <- 0 until repetitions) {
    val (rooted, dropped) = buildReplyTree(
      Seq(
        createComment(commentId = 111, 2000, postId),
        createReplyTo(commentId = 113, 3000, 112), // to evicted parent -> drop also
        createReplyTo(commentId = 112, 1000, 111), // to later post -> drop
      )
    )

    // execution:
    // 1. processElement(111)
    // 2. processBroadcastElement(113) (ts 3000, referring to past parent not yet received)
    // 3. processBroadcastElement(112)

    // 113 is rooted also! should be dropped as its parent is dropped

    assert(rooted.forall(_.postId == postId))
    assertResult(Set(111))(rooted.map(_.commentId).toSet)
    assertResult(Set(112, 113))(dropped.map(_.commentId).toSet)
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

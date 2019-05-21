package streams

import org.mvrs.dspa.model.RawCommentEvent
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.{FlatSpec, Matchers}

class BuildReplyTreeProcessFunctionTestSuite extends FlatSpec with Matchers {
  val postId = 999

  "reply resolver" must "create correct set" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val rawComments: List[RawCommentEvent] =
      List(
        createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId),
        createReplyTo(112, 2000, 111),
        createReplyTo(113, 3000, 112)
      )

    val replies = rawComments.filter(_.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildrenAndMaxTimestamp(
      replies, firstLevelCommentTimestamp,
      commentId => rawComments.filter(_.replyToCommentId.contains(commentId)),
      c => c.timestamp)

    println(result.mkString("\n"))

    assertResult(
      Set(
        (112, 2000),
        (113, 3000)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  it must "create correct set with siblings" in {

    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val rawComments: List[RawCommentEvent] =
      List(
        createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId),
        createReplyTo(211, 3000, 111),
        createReplyTo(212, 2000, 111),
        createReplyTo(311, 2500, 212),
        createReplyTo(312, 2500, 211)
      )

    val replies = rawComments.filter(_.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildrenAndMaxTimestamp(
      replies, firstLevelCommentTimestamp,
      commentId => rawComments.filter(_.replyToCommentId.contains(commentId)),
      c => c.timestamp)

    println(result.mkString("\n"))

    assertResult(
      Set(
        (211, 3000),
        (212, 2000),
        (311, 2500),
        (312, 3000)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  it must "create correct tree" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000

    val root = createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId)
    val r211 = createReplyTo(211, 3000, 111)
    val r212 = createReplyTo(212, 2000, 111)
    val r311 = createReplyTo(311, 2500, 212)
    val r312 = createReplyTo(312, 2500, 211)
    val r411 = createReplyTo(411, 2500, 312)

    val rawComments = List(root, r211, r212, r311, r312, r411)

    val expected = Set((311, 2500), (212, 2000), (411, 3000), (312, 3000), (211, 3000))

    val getChildren: Long => List[RawCommentEvent] = commentId => rawComments.filter(_.replyToCommentId.contains(commentId))

    val result2 = BuildReplyTreeProcessFunction.getWithChildrenAndMaxTimestamp(
      getChildren(root.commentId),
      root.timestamp,
      getChildren,
      c => c.timestamp)
      .map(t => (t._1.commentId, t._2))

    println(result2)
    assertResult(expected)(result2.toSet)
  }

  it must "create correct tree if unordered" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 2000
    val rawComments: List[RawCommentEvent] =
      List(
        createReplyTo(commentId = 114, 1000, 112), // min ts for valid parent: 3000
        createReplyTo(commentId = 115, 1000, 113), //          ""            : 4000
        createReplyTo(commentId = 112, 3000, 111), //          ""            : 3000
        createReplyTo(commentId = 113, 4000, 112), //          ""            : 4000
        createComment(commentId = firstLevelCommentId, firstLevelCommentTimestamp, postId),
      )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildrenAndMaxTimestamp(
      replies,
      firstLevelCommentTimestamp,
      commentId => rawComments.filter(_.replyToCommentId.contains(commentId)),
      c => c.timestamp)

    println(result.mkString("\n"))

    assertResult(4)(result.size)
    assertResult(
      Set(
        (112, 3000),
        (114, 3000),
        (113, 4000),
        (115, 4000)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  def createComment(commentId: Long, timestamp: Long, postId: Long, personId: Long = 1): RawCommentEvent =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, Some(postId), None, 0)

  def createReplyTo(commentId: Long, timestamp: Long, repliedToCommentId: Long, personId: Long = 1) =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, None, Some(repliedToCommentId), 0)

}

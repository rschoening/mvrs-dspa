package streams

import org.mvrs.dspa.model.RawCommentEvent
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.{FlatSpec, Matchers}

class BuildReplyTreeProcessFunctionTestSuite extends FlatSpec with Matchers {

  "reply resolver" must "create correct tree" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val rawComments: List[RawCommentEvent] =
      List(
        RawCommentEvent(commentId = firstLevelCommentId, personId, creationDate = DateTimeUtils.toDate(firstLevelCommentTimestamp), None, None, None, Some(postId), None, 0),
        RawCommentEvent(commentId = 112, personId, creationDate = DateTimeUtils.toDate(2000), None, None, None, None, Some(111), 0),
        RawCommentEvent(commentId = 113, personId, creationDate = DateTimeUtils.toDate(3000), None, None, None, None, Some(112), 0)
      )

    val replies = rawComments.filter(_.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildren(
      replies, firstLevelCommentTimestamp, c => rawComments.filter(_.replyToCommentId.contains(c.commentId)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (112, 2000),
        (113, 3000)
      )
    )(result.map(t => (t._1.commentId, t._2)))
  }

  "reply resolver" must "create correct tree with siblings" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val rawComments: List[RawCommentEvent] =
      List(
        RawCommentEvent(commentId = firstLevelCommentId, personId, creationDate = DateTimeUtils.toDate(firstLevelCommentId), None, None, None, Some(postId), None, 0),
        RawCommentEvent(commentId = 211, personId, creationDate = DateTimeUtils.toDate(3000), None, None, None, None, Some(111), 0),
        RawCommentEvent(commentId = 212, personId, creationDate = DateTimeUtils.toDate(2000), None, None, None, None, Some(111), 0),
        RawCommentEvent(commentId = 311, personId, creationDate = DateTimeUtils.toDate(2500), None, None, None, None, Some(212), 0),
        RawCommentEvent(commentId = 312, personId, creationDate = DateTimeUtils.toDate(2500), None, None, None, None, Some(211), 0)
      )

    val replies = rawComments.filter(_.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildren(
      replies, firstLevelCommentTimestamp, c => rawComments.filter(_.replyToCommentId.contains(c.commentId)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (211, 3000),
        (212, 2000),
        (311, 2500),
        (312, 3000)
      )
    )(result.map(t => (t._1.commentId, t._2)))
  }

  "reply resolver" must "create correct tree if unordered" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 2000
    val rawComments: List[RawCommentEvent] =
      List(
        RawCommentEvent(commentId = 114, personId, creationDate = DateTimeUtils.toDate(1000), None, None, None, None, Some(112), 0), // min ts for valid parent: 3000
        RawCommentEvent(commentId = 115, personId, creationDate = DateTimeUtils.toDate(1000), None, None, None, None, Some(113), 0), //          ""            : 4000
        RawCommentEvent(commentId = 112, personId, creationDate = DateTimeUtils.toDate(3000), None, None, None, None, Some(111), 0), //          ""            : 3000
        RawCommentEvent(commentId = 113, personId, creationDate = DateTimeUtils.toDate(4000), None, None, None, None, Some(112), 0), //          ""            : 4000
        RawCommentEvent(commentId = firstLevelCommentId, personId, creationDate = DateTimeUtils.toDate(firstLevelCommentTimestamp), None, None, None, Some(postId), None, 0),
      )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.getWithChildren(
      replies, firstLevelCommentTimestamp, c => rawComments.filter(_.replyToCommentId.contains(c.commentId)))

    println(result.mkString("\n"))

    assertResult(4)(result.size)
    assertResult(
      Set(
        (112, 3000),
        (114, 3000),
        (113, 4000),
        (115, 4000)
      )
    )(result.map(t => (t._1.commentId, t._2)))
  }

}

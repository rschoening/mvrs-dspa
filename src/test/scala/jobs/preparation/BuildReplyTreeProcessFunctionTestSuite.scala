package jobs.preparation

import org.mvrs.dspa.model.RawCommentEvent
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.{FlatSpec, Matchers}

class BuildReplyTreeProcessFunctionTestSuite extends FlatSpec with Matchers {

  "reply resolver" must "create correct tree" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val rawComments: List[RawCommentEvent] = List(
      RawCommentEvent(commentId = firstLevelCommentId, personId, creationDate = DateTimeUtils.toDate(1000), None, None, None, Some(postId), None, 0),
      RawCommentEvent(commentId = 112, personId, creationDate = DateTimeUtils.toDate(2000), None, None, None, None, Some(111), 0),
      RawCommentEvent(commentId = 113, personId, creationDate = DateTimeUtils.toDate(3000), None, None, None, None, Some(112), 0)
    )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.resolveDanglingReplies(
      replies, postId, c => rawComments.filter(_.replyToCommentId.contains(c.commentId)))

    println(result.mkString("\n"))

    assertResult(2)(result.size)
    assert(result.forall(_.postId == postId))
  }

  "reply resolver" must "create correct tree if unordered" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val rawComments: List[RawCommentEvent] = List(
      RawCommentEvent(commentId = 114, personId, creationDate = DateTimeUtils.toDate(1000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = 115, personId, creationDate = DateTimeUtils.toDate(1000), None, None, None, None, Some(113), 0),
      RawCommentEvent(commentId = 112, personId, creationDate = DateTimeUtils.toDate(3000), None, None, None, None, Some(111), 0),
      RawCommentEvent(commentId = 113, personId, creationDate = DateTimeUtils.toDate(4000), None, None, None, None, Some(112), 0),
      RawCommentEvent(commentId = firstLevelCommentId, personId, creationDate = DateTimeUtils.toDate(5000), None, None, None, Some(postId), None, 0),
    )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.resolveDanglingReplies(
      replies, postId, c => rawComments.filter(_.replyToCommentId.contains(c.commentId)))

    println(result.mkString("\n"))

    assertResult(4)(result.size)
    assert(result.forall(_.postId == postId))
  }

}

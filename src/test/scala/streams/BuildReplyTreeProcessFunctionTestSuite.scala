package streams

import org.mvrs.dspa.model.RawCommentEvent
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction._
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.{FlatSpec, Matchers}

class BuildReplyTreeProcessFunctionTestSuite extends FlatSpec with Matchers {
  val postId = 999

  "reply resolver" must "create correct set" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val rawComments =
      List(
        createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId),
        createReplyTo(112, 2000, 111),
        createReplyTo(113, 3000, 112)
      )

    val result = getDescendants(firstLevelCommentId, firstLevelCommentTimestamp,
      commentId => rawComments.filter(_.replyToCommentId.contains(commentId)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (112, true),
        (113, true)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  it must "create correct set with siblings" in {

    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000
    val comments =
      List(
        createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId),
        createReplyTo(211, 3000, 111),
        createReplyTo(212, 2000, 111),
        createReplyTo(311, 2500, 212),
        createReplyTo(312, 2500, 211)
      )

    val result = getDescendants(firstLevelCommentId, firstLevelCommentTimestamp,
      id => comments.filter(_.replyToCommentId.contains(id)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (211, true),
        (212, true),
        (311, true),
        (312, false)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  it must "create correct tree" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 1000

    val comments =
      List(
        createComment(firstLevelCommentId, firstLevelCommentTimestamp, postId),
        createReplyTo(211, 3000, 111),
        createReplyTo(212, 2000, 111),
        createReplyTo(311, 2500, 212),
        createReplyTo(312, 2500, 211),
        createReplyTo(411, 2500, 312),
      )

    val result = getDescendants(firstLevelCommentId, firstLevelCommentTimestamp,
      id => comments.filter(_.replyToCommentId.contains(id)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (211, true),
        (212, true),
        (311, true),
        (312, false),
        (411, false),
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  it must "create correct tree if unordered" in {
    val firstLevelCommentId = 111
    val firstLevelCommentTimestamp = 2000

    val comments =
      List(
        createReplyTo(commentId = 114, 1000, 112),
        createReplyTo(commentId = 115, 1000, 113),
        createReplyTo(commentId = 112, 3000, 111),
        createReplyTo(commentId = 113, 4000, 112),
        createReplyTo(commentId = 116, 4000, 114),
        createComment(commentId = firstLevelCommentId, firstLevelCommentTimestamp, postId),
      )

    val result = getDescendants(firstLevelCommentId, firstLevelCommentTimestamp,
      id => comments.filter(_.replyToCommentId.contains(id)))

    println(result.mkString("\n"))

    assertResult(
      Set(
        (112, true),
        (114, false),
        (113, true),
        (115, false),
        (116, true)
      )
    )(result.map(t => (t._1.commentId, t._2)).toSet)
  }

  def createComment(commentId: Long, timestamp: Long, postId: Long, personId: Long = 1): RawCommentEvent =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, Some(postId), None, 0)

  def createReplyTo(commentId: Long, timestamp: Long, repliedToCommentId: Long, personId: Long = 1) =
    RawCommentEvent(commentId, personId, creationDate = DateTimeUtils.toDate(timestamp), None, None, None, None, Some(repliedToCommentId), 0)

}

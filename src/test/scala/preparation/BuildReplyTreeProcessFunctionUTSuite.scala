package preparation

import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.preparation.BuildReplyTreeProcessFunction
import org.scalatest.{FlatSpec, Matchers}

class BuildReplyTreeProcessFunctionUTSuite extends FlatSpec with Matchers {

  "reply resolver" must "create correct tree" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val rawComments: List[CommentEvent] = List(
      CommentEvent(id = firstLevelCommentId, personId, creationDate = 1000, None, None, None, Some(postId), None, 0),
      CommentEvent(id = 112, personId, creationDate = 2000, None, None, None, None, Some(111), 0),
      CommentEvent(id = 113, personId, creationDate = 3000, None, None, None, None, Some(112), 0)
    )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.resolveDanglingReplies(
      replies, postId, c => rawComments.filter(_.replyToCommentId.contains(c.id)))

    println(result.mkString("\n"))

    assertResult(2)(result.size)
    assert(result.forall(c => c.replyToPostId.contains(postId)))
  }

  "reply resolver" must "create correct tree if unordered" in {

    val postId = 999
    val personId = 1
    val firstLevelCommentId = 111
    val rawComments: List[CommentEvent] = List(
      CommentEvent(id = 114, personId, creationDate = 1000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 115, personId, creationDate = 1000, None, None, None, None, Some(113), 0),
      CommentEvent(id = 112, personId, creationDate = 3000, None, None, None, None, Some(111), 0),
      CommentEvent(id = 113, personId, creationDate = 4000, None, None, None, None, Some(112), 0),
      CommentEvent(id = firstLevelCommentId, personId, creationDate = 5000, None, None, None, Some(postId), None, 0),
    )

    val replies = rawComments.filter(c => c.replyToCommentId.getOrElse(-1) == firstLevelCommentId)

    val result = BuildReplyTreeProcessFunction.resolveDanglingReplies(
      replies, postId, c => rawComments.filter(_.replyToCommentId.contains(c.id)))

    println(result.mkString("\n"))

    assertResult(4)(result.size)
    assert(result.forall(c => c.replyToPostId.contains(postId)))
  }

}

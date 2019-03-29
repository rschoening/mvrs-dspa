package org.mvrs.dspa.events

import scala.collection.Set

case class PostStatistics(postId: Long, time: Long, commentCount: Int, replyCount: Int, likeCount: Int, distinctUserCount: Int, newPost: Boolean)

trait ForumEvent {
  val personId: Long
  val creationDate: Long

  def postId: Long
}

final case class LikeEvent(personId: Long,
                           creationDate: Long,
                           postId: Long) extends ForumEvent {
}

final case class CommentEvent(id: Long,
                              personId: Long,
                              creationDate: Long,
                              locationIP: Option[String],
                              browserUsed: Option[String],
                              content: Option[String],
                              replyToPostId: Option[Long], // TODO make non-optional in persisted structure
                              replyToCommentId: Option[Long],
                              placeId: Int) extends ForumEvent {
  // TODO add learning test for zoneddatetime to (comparable) epoch timestamp: are these timestamps comparable when from different zones?

  def postId: Long = replyToPostId.get
}

final case class PostEvent(id: Long,
                           personId: Long,
                           creationDate: Long,
                           imageFile: Option[String],
                           locationIP: Option[String],
                           browserUsed: Option[String],
                           language: Option[String],
                           content: Option[String],
                           tags: Set[Int],
                           forumId: Long,
                           placeId: Int) extends ForumEvent {

  def postId: Long = id
}

object CommentEvent {
  def parse(line: String): CommentEvent = {
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"
    val tokens = line.split('|')

    assert(tokens.length == 9)

    CommentEvent(
      id = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toEpochMillis(tokens(2).trim),
      locationIP = ParseUtils.toOptionalString(tokens(3)),
      browserUsed = ParseUtils.toOptionalString(tokens(4)),
      content = ParseUtils.toOptionalString(tokens(5)),
      replyToPostId = ParseUtils.toOptionalLong(tokens(6)),
      replyToCommentId = ParseUtils.toOptionalLong(tokens(7)),
      placeId = tokens(8).trim.toInt)
  }
}

object LikeEvent {
  def parse(line: String): LikeEvent = {
    // Person.id|Post.id|creationDate
    val tokens = line.split('|')

    assert(tokens.length == 3)

    LikeEvent(
      personId = tokens(0).toLong,
      postId = tokens(1).toLong,
      creationDate = ParseUtils.toEpochMillis(tokens(2).trim))
  }
}

object PostEvent {
  def parse(line: String): PostEvent = {
    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    val tokens = line.split('|')

    assert(tokens.length == 11)

    PostEvent(
      id = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toEpochMillis(tokens(2).trim),
      imageFile = ParseUtils.toOptionalString(tokens(3)),
      locationIP = ParseUtils.toOptionalString(tokens(4)),
      browserUsed = ParseUtils.toOptionalString(tokens(5)),
      language = ParseUtils.toOptionalString(tokens(6)),
      content = ParseUtils.toOptionalString(tokens(7)),
      tags = ParseUtils.toSet(tokens(8)),
      forumId = tokens(9).toLong,
      placeId = tokens(10).trim.toInt)
  }
}


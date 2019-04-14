package org.mvrs.dspa.events

import java.time.{LocalDateTime, ZoneOffset}

import kantan.csv.DecodeError.TypeError
import kantan.csv.{CellDecoder, RowDecoder}

import scala.collection.Set

case class PostStatistics(postId: Long, time: Long, commentCount: Int, replyCount: Int, likeCount: Int, distinctUserCount: Int, newPost: Boolean)

trait ForumEvent {
  val personId: Long
  val creationDate: LocalDateTime
  val timestamp: Long = creationDate.toInstant(ZoneOffset.UTC).toEpochMilli
}

final case class LikeEvent(personId: Long,
                           creationDate: LocalDateTime,
                           postId: Long) extends ForumEvent {
}

final case class PostEvent(postId: Long,
                           personId: Long,
                           creationDate: LocalDateTime,
                           imageFile: Option[String],
                           locationIP: Option[String],
                           browserUsed: Option[String],
                           language: Option[String],
                           content: Option[String],
                           tags: Set[Int], // TODO requires a special decoder
                           forumId: Long,
                           placeId: Int) extends ForumEvent {
}

final case class RawCommentEvent(commentId: Long,
                                 personId: Long,
                                 creationDate: LocalDateTime,
                                 locationIP: Option[String],
                                 browserUsed: Option[String],
                                 content: Option[String],
                                 replyToPostId: Option[Long],
                                 replyToCommentId: Option[Long],
                                 placeId: Int) extends ForumEvent {
}

final case class CommentEvent(commentId: Long,
                              personId: Long,
                              creationDate: LocalDateTime,
                              locationIP: Option[String],
                              browserUsed: Option[String],
                              content: Option[String],
                              postId: Long,
                              replyToCommentId: Option[Long],
                              placeId: Int) extends ForumEvent {
}

//trait ForumEvent {
//  val personId: Long
//  val creationDate: Long
//
//  def postId: Long
//}
//
//
//final case class LikeEvent(personId: Long,
//                           creationDate: Long,
//                           postId: Long) extends ForumEvent {
//}
//
//final case class CommentEvent(id: Long,
//                              personId: Long,
//                              creationDate: Long,
//                              locationIP: Option[String],
//                              browserUsed: Option[String],
//                              content: Option[String],
//                              replyToPostId: Option[Long], // TODO make non-optional in persisted structure
//                              replyToCommentId: Option[Long],
//                              placeId: Int) extends ForumEvent {
//  // TODO add learning test for zoneddatetime to (comparable) epoch timestamp: are these timestamps comparable when from different zones?
//
//  def postId: Long = replyToPostId.get
//}
//
//final case class PostEvent(id: Long,
//                           personId: Long,
//                           creationDate: Long,
//                           imageFile: Option[String],
//                           locationIP: Option[String],
//                           browserUsed: Option[String],
//                           language: Option[String],
//                           content: Option[String],
//                           tags: Set[Int],
//                           forumId: Long,
//                           placeId: Int) extends ForumEvent {
//
//  def postId: Long = id
//}

object RawCommentEvent {
  def decoder: RowDecoder[RawCommentEvent] = {
    import kantan.csv.java8._
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawCommentEvent.apply)
  }

  def parse(line: String): RawCommentEvent = {
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"

    val tokens = line.split('|')

    assert(tokens.length == 9)

    RawCommentEvent(
      commentId = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toDateTime(tokens(2).trim),
      locationIP = ParseUtils.toOptionalString(tokens(3)),
      browserUsed = ParseUtils.toOptionalString(tokens(4)),
      content = ParseUtils.toOptionalString(tokens(5)),
      replyToPostId = ParseUtils.toOptionalLong(tokens(6)),
      replyToCommentId = ParseUtils.toOptionalLong(tokens(7)),
      placeId = tokens(8).trim.toInt)
  }
}

object LikeEvent {
  def decoder: RowDecoder[LikeEvent] = {
    import kantan.csv.java8._

    // Person.id|Post.id|creationDate
    RowDecoder.decoder(0, 2, 1)(LikeEvent.apply)
  }

  def parse(line: String): LikeEvent = {
    // Person.id|Post.id|creationDate
    val tokens = line.split('|')

    assert(tokens.length == 3)

    LikeEvent(
      personId = tokens(0).toLong,
      postId = tokens(1).toLong,
      creationDate = ParseUtils.toDateTime(tokens(2).trim))
  }
}

object PostEvent {
  def decoder: RowDecoder[PostEvent] = {
    import kantan.csv.java8._

    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    implicit val dateDecoder: CellDecoder[LocalDateTime] = CellDecoder.from(str => Right(ParseUtils.toDateTime(str)))
    implicit val setDecoder: CellDecoder[scala.collection.Set[Int]] = CellDecoder.from(toSet)
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)(PostEvent.apply)
  }

  private def toSet(str: String) = {
    try Right(ParseUtils.toSet(str))
    catch {
      case e: Exception => Left(new TypeError(e.getMessage))
    }
  }

  def parse(line: String): PostEvent = {
    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    val tokens = line.split('|')

    assert(tokens.length == 11)

    PostEvent(
      postId = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toDateTime(tokens(2).trim),
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


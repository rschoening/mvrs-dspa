package org.mvrs.dspa.events

import java.util.Date

import kantan.csv.DecodeError.TypeError
import kantan.csv.{CellDecoder, RowDecoder}

import scala.collection.Set


sealed trait TimestampedEvent {
  val creationDate: Date
  val timestamp: Long = creationDate.toInstant.toEpochMilli
}

sealed trait ForumEvent extends TimestampedEvent {
  val personId: Long
  val postId: Long
}

final case class LikeEvent(personId: Long,
                           creationDate: Date,
                           postId: Long) extends ForumEvent {
}

final case class PostEvent(postId: Long,
                           personId: Long,
                           creationDate: Date,
                           imageFile: Option[String],
                           locationIP: Option[String],
                           browserUsed: Option[String],
                           language: Option[String],
                           content: Option[String],
                           tags: Set[Int], // requires custom cell decoder
                           forumId: Long,
                           placeId: Int) extends ForumEvent {
}

final case class CommentEvent(commentId: Long,
                              personId: Long,
                              creationDate: Date,
                              locationIP: Option[String],
                              browserUsed: Option[String],
                              content: Option[String],
                              postId: Long,
                              replyToCommentId: Option[Long],
                              placeId: Int) extends ForumEvent {
}

final case class RawCommentEvent(commentId: Long,
                                 personId: Long,
                                 creationDate: Date,
                                 locationIP: Option[String],
                                 browserUsed: Option[String],
                                 content: Option[String],
                                 replyToPostId: Option[Long],
                                 replyToCommentId: Option[Long],
                                 placeId: Int) extends TimestampedEvent {
}

object RawCommentEvent {
  def csvDecoder: RowDecoder[RawCommentEvent] = {
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"
    //    implicit val dateDecoder: Decoder[String, Date, DecodeError, codecs.type] = ParseUtils.dateDecoder
    import ParseUtils.utcDateDecoder
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawCommentEvent.apply)
  }

  def parse(line: String): RawCommentEvent = {
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"

    val tokens = line.split('|')

    assert(tokens.length == 9)

    RawCommentEvent(
      commentId = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toUtcDate(tokens(2).trim),
      locationIP = ParseUtils.toOptionalString(tokens(3)),
      browserUsed = ParseUtils.toOptionalString(tokens(4)),
      content = ParseUtils.toOptionalString(tokens(5)),
      replyToPostId = ParseUtils.toOptionalLong(tokens(6)),
      replyToCommentId = ParseUtils.toOptionalLong(tokens(7)),
      placeId = tokens(8).trim.toInt)
  }
}

object LikeEvent {
  def csvDecoder: RowDecoder[LikeEvent] = {
    // Person.id|Post.id|creationDate
    import ParseUtils.utcDateDecoder
    RowDecoder.decoder(0, 2, 1)(LikeEvent.apply)
  }

  def parse(line: String): LikeEvent = {
    // Person.id|Post.id|creationDate
    val tokens = line.split('|')

    assert(tokens.length == 3)

    LikeEvent(
      personId = tokens(0).toLong,
      postId = tokens(1).toLong,
      creationDate = ParseUtils.toUtcDate(tokens(2).trim))
  }
}

object PostEvent {
  def csvDecoder: RowDecoder[PostEvent] = {
    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    import ParseUtils.utcDateDecoder
    implicit val setDecoder: CellDecoder[scala.collection.Set[Int]] = CellDecoder.from(toSet)
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)(PostEvent.apply)
  }

  def parse(line: String): PostEvent = {
    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    val tokens = line.split('|')

    assert(tokens.length == 11)

    PostEvent(
      postId = tokens(0).toLong,
      personId = tokens(1).toLong,
      creationDate = ParseUtils.toUtcDate(tokens(2).trim),
      imageFile = ParseUtils.toOptionalString(tokens(3)),
      locationIP = ParseUtils.toOptionalString(tokens(4)),
      browserUsed = ParseUtils.toOptionalString(tokens(5)),
      language = ParseUtils.toOptionalString(tokens(6)),
      content = ParseUtils.toOptionalString(tokens(7)),
      tags = ParseUtils.toSet(tokens(8)),
      forumId = tokens(9).toLong,
      placeId = tokens(10).trim.toInt)
  }

  private def toSet(str: String) = {
    try Right(ParseUtils.toSet(str))
    catch {
      case e: Exception => Left(new TypeError(e.getMessage))
    }
  }
}

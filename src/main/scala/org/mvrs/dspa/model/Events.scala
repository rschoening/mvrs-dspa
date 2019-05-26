package org.mvrs.dspa.model

import java.util.Date

import kantan.csv.DecodeError.TypeError
import kantan.csv.{CellDecoder, RowDecoder}
import org.mvrs.dspa.model.EventType.EventType
import org.mvrs.dspa.utils.ParseUtils

/**
  * Minimum event representation
  */
case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)

object EventType extends Enumeration {
  type EventType = Value
  val Post, Comment, Reply, Like = Value
}

/**
  * An event having a creation date, represented also as a timestamp value in milliseconds since the epoch
  */
sealed trait TimestampedEvent {
  val creationDate: Date
  val timestamp: Long = creationDate.toInstant.toEpochMilli
}

/**
  * An event related to a forum post
  */
sealed trait ForumEvent extends TimestampedEvent {
  val personId: Long
  val postId: Long
}

/**
  * Event of a person liking a post
  */
final case class LikeEvent(personId: Long,
                           creationDate: Date,
                           postId: Long) extends ForumEvent {
}

/**
  * Event of a person creating a new post
  */
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

/**
  * Event of a person commenting on a post, either directly, or by replying to an existing comment or reply
  */
final case class CommentEvent(commentId: Long,
                              personId: Long,
                              creationDate: Date,
                              locationIP: Option[String],
                              browserUsed: Option[String],
                              content: Option[String],
                              postId: Long,
                              replyToCommentId: Option[Long],
                              placeId: Int) extends ForumEvent {
  val isReply: Boolean = replyToCommentId.isDefined
}

/**
  * The raw comment event information available in the input data, which in case of replies references the post
  * indirectly, via the parent replies to the initial comment, which references the post directly.
  */
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
  /**
    * [[https://nrinaudo.github.io/kantan.csv/ Kantan-csv]]
    * decoder for decoding raw comments from lines in the test data format
    *
    * @return row decoder
    */
  def csvDecoder: RowDecoder[RawCommentEvent] = {
    // "id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId"
    //    implicit val dateDecoder: Decoder[String, Date, DecodeError, codecs.type] = ParseUtils.dateDecoder
    import org.mvrs.dspa.utils.ParseUtils.utcDateDecoder
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawCommentEvent.apply)
  }
}

object LikeEvent {
  /**
    * [[https://nrinaudo.github.io/kantan.csv/ Kantan-csv]]
    * decoder for decoding likes from lines in the test data format
    *
    * @return row decoder
    */
  def csvDecoder: RowDecoder[LikeEvent] = {
    // Person.id|Post.id|creationDate
    import ParseUtils.utcDateDecoder
    RowDecoder.decoder(0, 2, 1)(LikeEvent.apply)
  }
}

object PostEvent {
  /**
    * [[https://nrinaudo.github.io/kantan.csv/ Kantan-csv]]
    * decoder for decoding post events from lines in the test data format
    *
    * @return row decoder
    */
  def csvDecoder: RowDecoder[PostEvent] = {
    // id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
    import ParseUtils.utcDateDecoder
    implicit val setDecoder: CellDecoder[Set[Int]] = CellDecoder.from(toSet)
    RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)(PostEvent.apply)
  }

  private def toSet(str: String): Either[TypeError, Set[Int]] = {
    try Right(ParseUtils.toSet(str))
    catch {
      case e: Exception => Left(new TypeError(e.getMessage))
    }
  }
}

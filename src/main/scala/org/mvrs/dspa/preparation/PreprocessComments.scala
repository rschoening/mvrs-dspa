package org.mvrs.dspa.preparation

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import org.mvrs.dspa.events.CommentEvent

import scala.collection.mutable
import scala.language.reflectiveCalls

object PreprocessComments extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)

  val dict = mutable.Map[Long, ZonedDateTime]()

  // first pass: build dictionary comment id -> creation date
  using(io.Source.fromFile(filePath))(source => {
    for (line <- source.getLines.drop(1)) {
      val c = CommentEvent.parse(line)
      dict += c.id -> c.creationDate
    }
  })

  // second pass: collect negative time differences
  using(io.Source.fromFile(filePath))(source => {
    val unit = ChronoUnit.MILLIS

    var minDifference: Long = Long.MaxValue
    var maxDifference: Long = Long.MinValue
    var minDifferenceComment: Option[CommentEvent] = None

    for (line <- source.getLines.drop(1)) {
      val c = CommentEvent.parse(line)
      if (c.replyToPostId.isEmpty) {
        val childCreationDate = c.creationDate
        val parentId = c.replyToCommentId.get

        val parentCreationDate = dict.get(parentId)

        if (parentCreationDate.isEmpty) {
          println(s"comment ${c.id} replies to unknown comment $parentId")
        }
        else {
          val difference: Long = unit.between(parentCreationDate.get, childCreationDate)

          if (difference < minDifference) {
            minDifferenceComment = Some(c)
            minDifference = difference
          }
          maxDifference = math.max(maxDifference, difference)
        }
      }
    }

    val millisPerHour = 60.0 * 60.0 * 1000

    println(s"minimum difference: $minDifference milliseconds (${minDifference / millisPerHour} hours)")
    println(s"maximum difference: $maxDifference milliseconds (${maxDifference / millisPerHour} hours)")

    minDifferenceComment.foreach(
      c => {
        println(s"comment with minimum difference to replied-to comment: ${c.id}")
        println(s"replied to ${c.replyToCommentId.get}")
        println(s"comment details: $c")
      })


    /*
      id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId

      reply:
      4112780|866|2012-09-11T00:00:53Z|196.11.124.6|Firefox|About Mobutu Sese Seko, commonly known as Mobutu or Mobutu Sese Seko, was the President of the Democratic Republic of the Congo (also known as Zaire for much of his.||4112760|94

      comment replied to:
      4112760|626|2012-09-11T11:59:55Z|27.115.0.13|Internet Explorer|About Mobutu Sese Seko, 1997), commonly known as Mobutu or Mobutu Sese Seko, was the President of the Democratic Republic of the Congo (also.||4112740|73

      parent (comment): 2012-09-11T11:59:55Z
      child (reply):    2012-09-11T00:00:53Z
     */
  })

  /**
    * From the book, Beginning Scala, by David Pollak.
    * param is duck typed, can be anything with def close() : Unit
    */
  def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }

}

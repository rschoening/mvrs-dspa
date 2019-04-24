package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.events.RawCommentEvent

import scala.collection.mutable
import scala.language.reflectiveCalls

object AnalyseComments extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)

  val dict = mutable.Map[Long, Long]()
  val postByCommentId = mutable.Map[Long, Long]()
  var referenceToLaterComment = 0

  // first pass: build dictionary comment id -> creation date
  using(io.Source.fromFile(filePath))(source => {
    for (line <- source.getLines.drop(1)) {
      val c = RawCommentEvent.parse(line)
      dict += c.commentId -> c.timestamp
    }
  })

  // second pass: collect negative time differences
  using(io.Source.fromFile(filePath))(source => {
    var minDifference: Long = Long.MaxValue
    var maxDifference: Long = Long.MinValue
    var minDifferenceComment: Option[RawCommentEvent] = None

    for (line <- source.getLines.drop(1)) {

      val c = RawCommentEvent.parse(line)

      if (c.replyToPostId.isEmpty) {
        // it's a reply to a comment
        val childCreationDate = c.timestamp
        val parentId = c.replyToCommentId.get

        if (postByCommentId.get(parentId).isEmpty) {
          // the parent id has not yet been seen
          referenceToLaterComment += 1
        }
        else {
          postByCommentId += c.commentId -> postByCommentId(parentId)
        }

        val parentCreationDate = dict.get(parentId)

        if (parentCreationDate.isEmpty) {
          println(s"comment ${c.commentId} replies to unknown comment $parentId")
        }
        else {
          val difference: Long = childCreationDate - parentCreationDate.get

          if (difference < minDifference) {
            minDifferenceComment = Some(c)
            minDifference = difference
          }
          maxDifference = math.max(maxDifference, difference)
        }
      }
      else {
        postByCommentId += c.commentId -> c.replyToPostId.get
      }
    }

    val millisPerHour = 60.0 * 60.0 * 1000

    println(s"references to comments appearing later in file: $referenceToLaterComment") // 35606
    println(s"minimum difference: $minDifference milliseconds (${minDifference / millisPerHour} hours)")
    println(s"maximum difference: $maxDifference milliseconds (${maxDifference / millisPerHour} hours)")

    minDifferenceComment.foreach(
      c => {
        println(s"comment with minimum difference to replied-to comment: ${c.commentId}")
        println(s"replied to ${c.replyToCommentId.get}")
        println(s"comment details: $c")
      })


    // 1k_users_sorted: comment_event_stream.csv
    // - total rows: 632042
    // - there are 35606 replies for which at least one of the parent replies appears later in the file
    //
    // - LoadCommentEventsJob drops 34753 replies and resolves 597289 rooted replies/comments, with a WM interval of 1000 (no scaling)
    //   -> the job can resolve 853 additional replies
    //      => due to watermark interval (the number of unresolved can be further decreased by increasing the WM interval)
    //   - note above numbers are for parallelism = 1; for p=4, there are 598659 rooted replies/comments
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

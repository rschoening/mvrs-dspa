package org.mvrs.dspa.trials

import java.io.File
import java.time.ZonedDateTime

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.string.Trimmed
import org.mvrs.dspa.events.RawCommentEvent

object RefinedEventsTrial extends App {
  type LongId = Long Refined Positive
  type PlaceId = Int Refined Positive
  type Timestamp = Long Refined Positive
  type NonEmptyString = String Refined NonEmpty with Trimmed

  //println(parseTrial("1045510|129|2012-02-02T05:17:34Z|31.96.0.7|Firefox|About Vladimir Lenin, extensive theoretic and philosophical. About Kurt Weill, of capitalism, which included the ballad.|270250||28"))

  sealed trait ForumEvent {
    val personId: LongId
    val creationDate: Timestamp

    def postId: LongId
  }


  final case class RawComment(id: LongId,
                              personId: LongId,
                              creationDate: Timestamp,
                              locationIP: NonEmptyString,
                              browserUsed: NonEmptyString,
                              content: NonEmptyString,
                              replyToPostId: Option[LongId], // TODO make non-optional in persisted structure
                              replyToCommentId: Option[LongId],
                              placeId: PlaceId) extends ForumEvent {
    def postId: LongId = replyToPostId.get
  }

}

object NewEventsTrial extends App {
  type LongId = Long Refined NonNegative
  type PlaceId = Int Refined NonNegative
  type NonEmptyString = String Refined NonEmpty

  // NOTE it seems that Refined types cannot be used in Flink record classes:
  // class eu.timepit.refined.api.Refined cannot be used as a POJO type because not all fields are valid POJO fields

  // would probably require custom serde classes, that import eu.timepit.refined.auto._

  final case class RawComment(id: LongId, // 1 row has id = 0
                              personId: LongId, // apparently this can be 0
                              creationDate: ZonedDateTime,
                              locationIP: NonEmptyString,
                              browserUsed: NonEmptyString,
                              content: NonEmptyString,
                              replyToPostId: Option[LongId], // 4 references to postId = 0
                              replyToCommentId: Option[LongId], // 1 reference to comment id = 0
                              placeId: PlaceId) {
    val timestamp: Long = creationDate.toInstant.toEpochMilli
  }

  val path = "C:\\data\\dspa\\project\\1k-users-sorted\\streams\\comment_event_stream.csv"
  readFileRaw(path)
  readFileParse(path)
  readFileKantan(path)

  def readFileKantan(path: String): Unit = {
    val file: File = new java.io.File(path)

    // 1045530|919|2012-02-02T02:45:14Z|31.10.32.4|Chrome|About Vladimir Lenin, to create a socialist economic system. As a politician, Lenin was a persuasive and charismatic orator. As an intellectual his. About Kurt Weill, he developed productions such as his most well known work The Threepenny Opera, a Marxist critique of capitalism, which included the.||1045500|28
    import kantan.csv._
    import kantan.csv.java8._
    import kantan.csv.ops._
    import kantan.csv.refined._
    implicit val decoder: RowDecoder[RawComment] = RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawComment.apply)

    // implicit val decoder : RowDecoder[RawComment] = RowDecoder[RawComment]()
    val reader: CsvReader[ReadResult[RawComment]] = file.asCsvReader[RawComment](rfc.withHeader.withCellSeparator('|'))

    // reader.filter(_.isLeft).foreach(println)

    val start = System.nanoTime()

    val count = reader.count(_.isRight)
    reader.close()

    val millis = (System.nanoTime() - start) / 1000000.0

    println("KANTAN:")
    println(s"lines: $count")
    println(s"Milliseconds: $millis")
    println(s"Lines per second: ${count / (millis / 1000.0)}")
  }

  def readFileRaw(path: String): Unit = {
    import scala.io.Source
    val source = Source.fromFile(path)

    val start = System.nanoTime()

    val count = source.getLines().count(_ => true)

    source.close()

    val millis = (System.nanoTime() - start) / 1000000.0

    println("RAW:")
    println(s"lines: $count")
    println(s"Milliseconds: $millis")
    println(s"Lines per second: ${count / (millis / 1000.0)}")
  }

  def readFileParse(path: String): Unit = {
    import scala.io.Source
    val source = Source.fromFile(path)

    val start = System.nanoTime()

    val comments = source.getLines.drop(1).map(l => RawCommentEvent.parse(l))
    val count = comments.count(_ => true)

    source.close()

    val millis = (System.nanoTime() - start) / 1000000.0

    println("PARSE:")
    println(s"lines: $count")
    println(s"Milliseconds: $millis")
    println(s"Lines per second: ${count / (millis / 1000.0)}")
  }

}

object Car extends App {
  type NonEmptyString = String Refined NonEmpty with Trimmed

  final case class Car2(make: String, year: Option[Int], model: String, price: Float)

  val str = "1996,Jeep,Grand,xyz,99.9\n1996x,Jeep,Grand,xyz,99.9a\n,Jeep,Grand,xyz,11.11"

  decode(str)

  def decode(str: String): Unit = {
    import kantan.csv._
    import kantan.csv.ops._
    // import java.io._

    // implicit val stringInput: CsvSource[String] = CsvSource[Reader].contramap(s ⇒ new StringReader(s))
    // val reader = str.asCsvReader[Car2](rfc.withHeader)

    implicit val car2Decoder: RowDecoder[Car2] = RowDecoder.decoder(1, 0, 2, 4)(Car2.apply)

    val csvConfig = rfc.withoutHeader.withCellSeparator(',')

    str.asCsvReader[Car2](csvConfig).foreach {
      case Right(car) => println(car)
      case Left(e) => println(s"ERROR: $e")
    }
  }
}


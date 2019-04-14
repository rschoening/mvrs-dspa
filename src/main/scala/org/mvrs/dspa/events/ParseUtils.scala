package org.mvrs.dspa.events

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import kantan.codecs.Decoder
import kantan.csv.{CellDecoder, DecodeError, codecs}

object ParseUtils {
  private val formatter = DateTimeFormatter.ISO_DATE_TIME

  def dateDecoder: Decoder[String, LocalDateTime, DecodeError, codecs.type] = CellDecoder.from(s => Right(toDateTime(s)))

  def toSet(str: String): Set[Int] = str match {
    case "" | null => Set()
    case _ => str // TODO regex-based parsing
      .replace("[", "")
      .replace("]", "")
      .split(',')
      .map(_.trim.toInt)
      .toSet
  }

  def toEpochMillis(str: String): Long = ZonedDateTime.parse(str, formatter).toInstant.toEpochMilli

  // NOTE due to the problems with LocalDateTime in flink (and apparently elsewhere, see https://github.com/twitter/chill/issues/251)
  //      currently the event classes use LocalDateTime in UTC
  def toDateTime(str: String): LocalDateTime = ZonedDateTime.parse(str, formatter).withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime

  def toOptionalString(str: String): Option[String] = str match {
    case "" | null => None
    case _ => Some(str.trim)
  }

  def toOptionalLong(str: String): Option[Long] = str match {
    case "" | null => None
    case _ => Some(str.trim.toLong) // fail if not a long
  }
}

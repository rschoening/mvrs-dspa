package org.mvrs.dspa.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Date

import kantan.codecs.Decoder
import kantan.csv.{CellDecoder, DecodeError, codecs}

/**
  * Helper methods for parsing strings
  */
object ParseUtils {
  private val formatter = DateTimeFormatter.ISO_DATE_TIME

  implicit def utcDateDecoder: Decoder[String, Date, DecodeError, codecs.type] = CellDecoder.from(s => Right(toUtcDate(s)))

  def toSet(str: String): Set[Int] = str match {
    case "" | null => Set()
    case _ => str // TODO regex-based parsing
      .replace("[", "")
      .replace("]", "")
      .split(',')
      .map(_.trim.toInt)
      .toSet
  }

  def toEpochMillis(zonedDateTime: String): Long = ZonedDateTime.parse(zonedDateTime, formatter).toInstant.toEpochMilli

  def toUtcLocalDateTime(zonedDateTime: String): LocalDateTime =
    ZonedDateTime.parse(zonedDateTime, formatter).withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime

  // since flink can't serialize ZonedDateTime/LocalDateTime natively, event classes currently use java.util.Date in UTC
  def toUtcDate(str: String): Date =
    Date.from(ZonedDateTime.parse(str, formatter).withZoneSameInstant(ZoneId.of("UTC")).toInstant)

  def toOptionalString(str: String): Option[String] = str match {
    case "" | null => None
    case _ => Some(str.trim)
  }

  def toOptionalLong(str: String): Option[Long] = str match {
    case "" | null => None
    case _ => Some(str.trim.toLong) // fail if not a long
  }
}

package org.mvrs.dspa.events

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object ParseUtils {
  private val formatter = DateTimeFormatter.ISO_DATE_TIME

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

  def toDate(str: String): ZonedDateTime = ZonedDateTime.parse(str, formatter)

  def toOptionalString(str: String): Option[String] = str match {
    case "" | null => None
    case _ => Some(str.trim)
  }

  def toOptionalLong(str: String): Option[Long] = str match {
    case "" | null => None
    case _ => Some(str.trim.toLong) // fail if not a long
  }
}

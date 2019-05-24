package org.mvrs.dspa.utils

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date

import org.apache.commons.lang3.time.DurationFormatUtils

/**
  * Utilities for date/time conversion
  */
object DateTimeUtils {
  private val dateFormat =
    ThreadLocal.withInitial[SimpleDateFormat](() => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
  private val shortDateFormat =
    ThreadLocal.withInitial[SimpleDateFormat](() => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))

  def toDate(millis: Long): Date = Date.from(Instant.ofEpochMilli(millis))

  def toDateTime(millis: Long): LocalDateTime =
    LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

  def formatDuration(millis: Long, shortFormat: Boolean = false): String =
    DurationFormatUtils.formatDuration(millis, if (shortFormat) "HH:mm:ss" else "HH:mm:ss,SSS")

  def formatTimestamp(timestamp: Long, shortFormat: Boolean = false): String =
    (if (shortFormat) shortDateFormat else dateFormat).get.format(new Date(timestamp))
}

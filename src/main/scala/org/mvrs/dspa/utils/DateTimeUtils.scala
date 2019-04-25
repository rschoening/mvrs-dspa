package org.mvrs.dspa.utils

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date

import org.apache.commons.lang3.time.DurationFormatUtils

object DateTimeUtils {
  private val dateFormat =
    ThreadLocal.withInitial[SimpleDateFormat](() => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

  def toDate(millis: Long): Date = Date.from(Instant.ofEpochMilli(millis))

  def toDateTime(millis: Long): LocalDateTime =
    LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

  def formatDuration(millis: Long): String = DurationFormatUtils.formatDuration(millis, "HH:mm:ss,SSS")

  def formatTimestamp(timestamp: Long): String = dateFormat.get.format(new Date(timestamp))
}

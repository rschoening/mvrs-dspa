package org.mvrs.dspa

// temporary parking for central configuration settings
object Settings {
  val UnusualActivityControlFilePath = "c:\\temp\\activity-classification.txt"
  val tablesDirectory = "C:\\data\\dspa\\project\\1k-users-sorted\\tables"
  val streamsDirectory = "C:\\data\\dspa\\project\\1k-users-sorted\\streams"
  val likesStreamCsvPath: String = streamsDirectory + "\\likes_event_stream.csv"
  val postStreamCsvPath: String = streamsDirectory + "\\post_event_stream.csv"
  val commentStreamCsvPath: String = streamsDirectory + "\\comment_event_stream.csv"
}

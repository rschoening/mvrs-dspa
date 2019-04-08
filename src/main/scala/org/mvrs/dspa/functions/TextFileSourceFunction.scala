package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils

import scala.io.{BufferedSource, Source}


class TextFileSourceFunction[OUT](filePath: String,
                                  skipFirstLine: Boolean,
                                  parse: String => OUT,
                                  extractEventTime: OUT => Long,
                                  speedupFactor: Double,
                                  maximumDelayMillis: Int,
                                  delay: OUT => Long)
  extends ReplayedSourceFunction[String, OUT](parse, extractEventTime, speedupFactor, maximumDelayMillis, delay) {

  @volatile private var source: BufferedSource = _

  def this(filePath: String,
           skipFirstLine: Boolean,
           parse: String => OUT,
           extractEventTime: OUT => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Int = 0) =
    this(filePath, skipFirstLine, parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override def open(parameters: Configuration): Unit = {
    source = Source.fromFile(filePath)

    // consider using org.apache.flink.api.java.io.CsvInputFormat
    // var format = new org.apache.flink.api.java.io.TupleCsvInputFormat[OUT](filePath)
    // then ... format.openInputFormat() ... format.nextRecord()
    // this would support reading in parallel from splits, which we don't want here
  }

  override def close(): Unit = source.close()

  override protected def inputIterator: Iterator[String] = if (skipFirstLine) source.getLines().drop(1) else source.getLines()
}


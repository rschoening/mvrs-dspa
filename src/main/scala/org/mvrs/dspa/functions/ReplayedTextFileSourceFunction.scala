package org.mvrs.dspa.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils.FlinkUtils

import scala.io.{BufferedSource, Source}


class ReplayedTextFileSourceFunction[OUT](filePath: String,
                                          skipFirstLine: Boolean,
                                          parse: String => OUT,
                                          extractEventTime: OUT => Long,
                                          speedupFactor: Double,
                                          maximumDelayMillis: Int,
                                          delay: OUT => Long,
                                          watermarkInterval: Int)
  extends ReplayedSourceFunction[String, OUT](parse, extractEventTime, speedupFactor, maximumDelayMillis, delay, watermarkInterval, 1000) {

  @transient private var source: Option[BufferedSource] = None

  def this(filePath: String,
           skipFirstLine: Boolean,
           parse: String => OUT,
           extractEventTime: OUT => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Int = 0,
           watermarkInterval: Int = 1000) =
    this(filePath, skipFirstLine, parse, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds),
      watermarkInterval)

  override def open(parameters: Configuration): Unit = {
    source = Some(Source.fromFile(filePath))

    // consider using org.apache.flink.api.java.io.CsvInputFormat
    // var format = new org.apache.flink.api.java.io.TupleCsvInputFormat[OUT](filePath)
    // then ... format.openInputFormat() ... format.nextRecord()
    // this would support reading in parallel from splits, which we don't want here
  }


  override def run(ctx: SourceFunction.SourceContext[OUT]): Unit = {
    try super.run(ctx)
    finally closeSource()
  }

  override def cancel(): Unit = {
    super.cancel()
    closeSource()
  }

  override def close(): Unit = closeSource()

  private def closeSource(): Unit = {
    source.foreach(_.close())
    source = None
  }

  override protected def inputIterator: Iterator[String] = source.map(s => if (skipFirstLine) s.getLines().drop(1) else s.getLines()).get
}


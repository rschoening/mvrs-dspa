package org.mvrs.dspa.functions

import java.net.URI
import java.nio.charset.Charset

import kantan.csv._
import kantan.csv.ops._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils.FlinkUtils

import scala.io.Codec

/**
  * Source function for reading from csv file and applying a speedup factor and optionally, a delay to the emitted
  * elements.
  *
  * @inheritdoc
  * @param filePath                path to csv file
  * @param skipFirstLine           indicates if the first (header) line should be skipped
  * @param cellSeparator           the csv cell separator character
  * @param extractEventTime        the function to extract the event time from a parsed element
  * @param speedupFactor           the speedup factor relative to the event times
  * @param maximumDelayMillis      the upper bound to the expected delays (non-late elements). The emitted watermark will be based on this value.
  * @param delay                   the function to determine the delay based on a parsed element. For unit testing, a function can be passed that
  *                                deterministically returns delays for given elements
  * @param watermarkIntervalMillis the interval for emitting watermarks
  * @param charsetName             the java.nio charset name. Default: UTF8
  * @param rowDecoder              required implicit row decoder for the type (kantan.csv)
  * @tparam OUT type of emitted elements
  */
class ReplayedCsvFileSourceFunction[OUT: HeaderDecoder](filePath: String,
                                                        skipFirstLine: Boolean,
                                                        cellSeparator: Char,
                                                        extractEventTime: OUT => Long,
                                                        speedupFactor: Double,
                                                        maximumDelayMillis: Long,
                                                        delay: OUT => Long,
                                                        watermarkIntervalMillis: Long,
                                                        charsetName: Option[String])(implicit rowDecoder: RowDecoder[OUT])
  extends ReplayedSourceFunction[OUT, OUT](
    identity[OUT],
    extractEventTime,
    speedupFactor,
    maximumDelayMillis,
    delay,
    watermarkIntervalMillis,
    1000
  ) {
  @transient private var csvReader: CsvReader[ReadResult[OUT]] = _

  def this(filePath: String,
           skipFirstLine: Boolean,
           cellSeparator: Char,
           extractEventTime: OUT => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Long = 0,
           watermarkInterval: Long = 1000,
           charsetName: Option[String] = None)(implicit rowDecoder: RowDecoder[OUT]) =
    this(filePath, skipFirstLine, cellSeparator, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds)(),
      watermarkInterval, charsetName)

  override def open(parameters: Configuration): Unit = {
    val file = new java.io.File(new URI(filePath))
    val config = (if (skipFirstLine) rfc.withHeader() else rfc).withCellSeparator(cellSeparator)

    implicit val coded: Codec = charsetName.map(name => Codec(Charset.forName(name))).getOrElse(Codec.UTF8)

    csvReader = file.asCsvReader[OUT](config)
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

  override protected def inputIterator: Iterator[OUT] =
    if (csvReader == null) Iterator[OUT]()
    else csvReader.map {
      case row@Left(error) => reportRowError(error); row // to be dropped in collect
      case row => row
    }.collect {
      case Right(out) => out
    }.toIterator

  private def closeSource(): Unit = {
    if (csvReader != null) csvReader.close()
  }
}


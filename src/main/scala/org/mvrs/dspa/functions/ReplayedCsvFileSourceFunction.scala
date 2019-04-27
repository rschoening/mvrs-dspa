package org.mvrs.dspa.functions

import java.net.URI

import kantan.csv._
import kantan.csv.ops._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.mvrs.dspa.functions.ReplayedSourceFunction._
import org.mvrs.dspa.utils.FlinkUtils

class ReplayedCsvFileSourceFunction[OUT: HeaderDecoder](filePath: String,
                                                        skipFirstLine: Boolean,
                                                        cellSeparator: Char,
                                                        extractEventTime: OUT => Long,
                                                        speedupFactor: Double,
                                                        maximumDelayMillis: Int,
                                                        delay: OUT => Long,
                                                        watermarkInterval: Long)(implicit rowDecoder: RowDecoder[OUT])
  extends ReplayedSourceFunction[OUT, OUT](identity[OUT], extractEventTime, speedupFactor, maximumDelayMillis, delay, watermarkInterval) {

  @volatile private var csvReader: Option[CsvReader[ReadResult[OUT]]] = None

  def this(filePath: String,
           skipFirstLine: Boolean,
           cellSeparator: Char,
           extractEventTime: OUT => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Int = 0,
           watermarkInterval: Long = 1000)(implicit rowDecoder: RowDecoder[OUT]) =
    this(filePath, skipFirstLine, cellSeparator, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: OUT) => 0L
      else (_: OUT) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds),
      watermarkInterval)

  override def open(parameters: Configuration): Unit = {
    val uri = new URI(filePath)
    val file = new java.io.File(uri)
    val config = (if (skipFirstLine) rfc.withHeader() else rfc).withCellSeparator(cellSeparator)
    csvReader = Some(file.asCsvReader[OUT](config))
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
    csvReader.map(_.map {
      case row@Left(error) => reportRowError(error); row // to be dropped in collect
      case row => row
    }
      .collect {
        case Right(out) => out
      })
      .getOrElse(List[OUT]())
      .toIterator

  private def closeSource(): Unit = {
    csvReader.foreach(_.close())
    csvReader = None
  }
}


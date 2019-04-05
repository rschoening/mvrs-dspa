package org.mvrs.dspa.functions

import java.io.{BufferedWriter, File, FileWriter, Writer}

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class SimpleTextFileSinkFunction(pathPrefix: String) extends RichSinkFunction[String] {
  private var writer: Option[Writer] = None

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val index: Int = getRuntimeContext.getIndexOfThisSubtask

    if (writer.isEmpty) {
      writer = Some(createWriter(pathPrefix, index))
    }
    writer.foreach(_.write(s"$value\n"))
  }

  private def createWriter(path: String, index: Int): BufferedWriter = {
    val fullPath = s"$path-$index.txt"
    new BufferedWriter(new FileWriter(new File(fullPath)))
  }

  // note: flush happens only when stream ends
  override def close(): Unit = {
    writer.foreach(_.close())
    writer = None
  }
}

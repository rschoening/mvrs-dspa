package org.mvrs.dspa.functions

import java.io.{BufferedWriter, File, FileWriter, Writer}

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Simplest-possible text file sink, used for debugging
  *
  * @param pathPrefix the prefix of the output files (one per parallel task)
  */
class SimpleTextFileSinkFunction(pathPrefix: String) extends RichSinkFunction[String] {
  @transient private var writer: Writer = _ // NOTE Option[Writer] observed to be set to null with @transient ... = None

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val index: Int = getRuntimeContext.getIndexOfThisSubtask

    if (writer == null) writer = createWriter(pathPrefix, index)

    writer.write(s"$value\n")
  }

  private def createWriter(path: String, index: Int): BufferedWriter = {
    val fullPath = s"$path-$index.txt"
    new BufferedWriter(new FileWriter(new File(fullPath)))
  }

  // note: flush happens only when stream ends
  override def close(): Unit = {
    if (writer != null) writer.close()
    writer = null
  }
}

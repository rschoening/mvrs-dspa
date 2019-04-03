package org.mvrs.dspa.functions

import java.io.{BufferedWriter, File, FileWriter, Writer}

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable

class TestingFileSinkFunction(pathPrefix: String) extends RichSinkFunction[String] {
  private val filesByIndex: mutable.Map[Int, Writer] = mutable.Map()

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val index: Int = getRuntimeContext.getIndexOfThisSubtask

    val writer = filesByIndex.getOrElseUpdate(index, createWriter(pathPrefix, index))
    assert(filesByIndex.size == 1) // expect instance per subtask
    writer.write(s"$value\n")
  }

  private def createWriter(path: String, index: Int): BufferedWriter = {
    val fullPath = s"$path-$index.txt"
    new BufferedWriter(new FileWriter(new File(fullPath)))
  }

  // note: flush happens only when stream ends
  override def close(): Unit = filesByIndex.values.foreach(_.close())
}

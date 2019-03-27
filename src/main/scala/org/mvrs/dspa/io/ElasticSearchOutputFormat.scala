package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

abstract class ElasticSearchOutputFormat[T](uri: String) extends OutputFormat[T] {

  private var client: Option[ElasticClient] = None

  // TODO add support for batching?
  def process(record: T, client: ElasticClient): Unit

  override def configure(parameters: Configuration): Unit = {}

  override def open(taskNumber: Int, numTasks: Int): Unit = client = Some(ElasticClient(ElasticProperties(uri)))

  override def writeRecord(record: T): Unit = process(record, client.get) // exception if None

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }
}

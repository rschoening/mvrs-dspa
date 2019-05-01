package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

abstract class ElasticSearchOutputFormat[T](nodes: ElasticSearchNode*) extends OutputFormat[T] {

  private var client: Option[ElasticClient] = None

  protected def process(record: T, client: ElasticClient): Unit

  protected def closing(client: ElasticClient): Unit = {}

  override def configure(parameters: Configuration): Unit = {}

  override def open(taskNumber: Int, numTasks: Int): Unit = client = Some(createClient(nodes: _*))

  override def writeRecord(record: T): Unit = process(record, client.get) // exception if None

  override def close(): Unit = {
    client.foreach(c => {
      closing(c)
      c.close()
    })
    client = None
  }
}

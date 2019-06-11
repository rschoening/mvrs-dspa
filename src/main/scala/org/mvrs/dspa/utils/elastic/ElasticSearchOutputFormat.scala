package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.mvrs.dspa.utils.elastic

/**
  * Base class for elastic4s based output formats
  *
  * @param nodes The ElasticSearch nodes to connect to
  * @tparam T The record type
  */
abstract class ElasticSearchOutputFormat[T](nodes: Seq[ElasticSearchNode]) extends OutputFormat[T] {

  @transient private lazy val client: ElasticClient = elastic.createClient(nodes: _*)

  override def open(taskNumber: Int, numTasks: Int): Unit = {}

  protected def process(record: T, client: ElasticClient): Unit

  protected def closing(client: ElasticClient): Unit = {}

  override def configure(parameters: Configuration): Unit = {}

  override def writeRecord(record: T): Unit = process(record, client)

  override def close(): Unit = {
    closing(client)
    client.close()
  }

}


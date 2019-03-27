package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

abstract class ElasticSearchSinkFunction[IN](uri: String, indexName: String, typeName: String) extends RichSinkFunction[IN] {
  private var client: Option[ElasticClient] = None

  override def open(parameters: Configuration): Unit = client = Some(ElasticClient(ElasticProperties(uri)))

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }

  override def invoke(value: IN, context: SinkFunction.Context[_]): Unit = process(value, client.get, context)

  def process(record: IN, client: ElasticClient, context: SinkFunction.Context[_]): Unit
}

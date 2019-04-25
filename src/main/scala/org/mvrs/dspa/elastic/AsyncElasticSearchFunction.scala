package org.mvrs.dspa.elastic

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.mvrs.dspa.utils.ElasticSearchUtils

abstract class AsyncElasticSearchFunction[IN, OUT](nodes: ElasticSearchNode*) extends RichAsyncFunction[IN, OUT] {
  require(nodes.nonEmpty, "at least one node must be provided")

  private var client: Option[ElasticClient] = None

  override def open(parameters: Configuration): Unit = client = Some(ElasticSearchUtils.createClient(nodes: _*))

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }

  override def asyncInvoke(input: IN, resultFuture: ResultFuture[OUT]): Unit = {
    asyncInvoke(client.get, input, resultFuture)
  }

  def asyncInvoke(client: ElasticClient, input: IN, resultFuture: ResultFuture[OUT]): Unit
}

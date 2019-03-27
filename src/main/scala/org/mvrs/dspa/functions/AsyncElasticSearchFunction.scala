package org.mvrs.dspa.functions

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

abstract class AsyncElasticSearchFunction[IN, OUT](elasticSearchUri: String) extends RichAsyncFunction[IN, OUT] {
  private var client: Option[ElasticClient] = None

  override def open(parameters: Configuration): Unit = client = Some(ElasticClient(ElasticProperties(elasticSearchUri)))

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }

  override def asyncInvoke(input: IN, resultFuture: ResultFuture[OUT]): Unit = {
    asyncInvoke(client.get, input, resultFuture)
  }

  def asyncInvoke(client: ElasticClient, input: IN, resultFuture: ResultFuture[OUT]): Unit
}

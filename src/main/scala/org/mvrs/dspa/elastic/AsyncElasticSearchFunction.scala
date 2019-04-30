package org.mvrs.dspa.elastic

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.mvrs.dspa.utils.ElasticSearchUtils

import scala.concurrent.ExecutionContext

abstract class AsyncElasticSearchFunction[IN, OUT: TypeInformation](nodes: ElasticSearchNode*)
  extends RichAsyncFunction[IN, OUT] with ResultTypeQueryable[OUT] {
  require(nodes.nonEmpty, "at least one node must be provided")

  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

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

  def getProducedType: TypeInformation[OUT] = createTypeInformation[OUT] // ensure non-generic serialization
}

package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.mvrs.dspa.utils.elastic

import scala.concurrent.ExecutionContext

abstract class AsyncElasticSearchFunction[IN, OUT: TypeInformation](nodes: Seq[ElasticSearchNode])
  extends RichAsyncFunction[IN, OUT] with ResultTypeQueryable[OUT] {
  require(nodes.nonEmpty, "at least one node must be provided")

  @transient implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
  @transient private var client: ElasticClient = _

  final override def open(parameters: Configuration): Unit = {
    client = elastic.createClient(nodes: _*)

    openCore(parameters)
  }

  protected def openCore(parameters: Configuration): Unit = {}

  final override def close(): Unit = {
    if (client != null) client.close()
    client = null
  }

  final override def asyncInvoke(input: IN, resultFuture: ResultFuture[OUT]): Unit = {
    asyncInvoke(client, input, resultFuture)
  }

  def asyncInvoke(client: ElasticClient, input: IN, resultFuture: ResultFuture[OUT]): Unit

  def getProducedType: TypeInformation[OUT] = createTypeInformation[OUT] // ensure non-generic serialization
}

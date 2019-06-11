package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import javax.annotation.Nonnegative

import scala.collection.mutable

/**
  * Base class for elastic4s-based output formats that support bulk requests
  *
  * @param indexName The ElasticSearch index name
  * @param typeName  The ElasticSearch type name
  * @param nodes     The ElasticSearch nodes to connect to
  * @param batchSize The maximum number of requests to submit in one batch
  * @tparam T The record type
  */
abstract class ElasticSearchBulkOutputFormat[T](indexName: String,
                                                typeName: String,
                                                nodes: Seq[ElasticSearchNode],
                                                @Nonnegative batchSize: Int = 1000)
  extends ElasticSearchOutputFormat[T](nodes) {
  require(batchSize > 0, "batch size must be > 0")
  @transient private lazy val requests = mutable.ArrayBuffer[BulkCompatibleRequest]()

  final override def process(record: T, client: ElasticClient): Unit = {
    requests.append(createRequest(record))

    if (requests.size >= batchSize) executeBatch(client)
  }

  override protected def closing(client: ElasticClient): Unit = if (requests.nonEmpty) executeBatch(client)

  protected def createRequest(record: T): BulkCompatibleRequest

  private def executeBatch(client: ElasticClient): Unit = {
    client.execute {
      bulk {
        requests
      }
    }.await // NOTE make non-blocking for larger-scale use - first attempts were unsuccessful though (connection exceptionally closed etc.)

    requests.clear()
  }
}

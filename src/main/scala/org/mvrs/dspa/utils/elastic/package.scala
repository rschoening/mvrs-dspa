package org.mvrs.dspa.utils

import com.sksamuel.elastic4s.http.index.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticNodeEndpoint, ElasticProperties, Response}

/**
  * Types and helper methods for reading and writing to ElasticSearch from Flink jobs. Based on [[https://github.com/sksamuel/elastic4s/blob/master/README.md elastic4s]]
  */
package object elastic {
  def createClient(nodes: ElasticSearchNode*) = ElasticClient(ElasticProperties(nodes.map(createEndpoint)))

  def dropIndex(client: ElasticClient, indexName: String): Response[DeleteIndexResponse] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._ // import the dsl

    client.execute {
      deleteIndex(indexName)
    }.await
  }

  private def createEndpoint(node: ElasticSearchNode) =
    ElasticNodeEndpoint(node.scheme, node.hostname, node.port, None)
}

package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.index.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticNodeEndpoint, ElasticProperties, Response}

object ElasticSearchUtils {
  def createClient(nodes: ElasticSearchNode*) = ElasticClient(ElasticProperties(nodes.map(createEndpoint)))

  def dropIndex(client: ElasticClient, indexName: String): Response[DeleteIndexResponse] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._ // import the dsl

    client.execute {
      deleteIndex(indexName)
    }.await
  }

  private def createEndpoint(node: ElasticSearchNode) = ElasticNodeEndpoint(node.scheme, node.hostname, node.port, None)
}

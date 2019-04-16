package org.mvrs.dspa.utils

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticNodeEndpoint, ElasticProperties}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.apache.http.HttpHost
import org.mvrs.dspa.utils

case class ElasticSearchNode(hostname: String, port: Int = 9200, scheme: String = "http") {
  def httpHost: HttpHost = new HttpHost(hostname, port, scheme)
}

/**
  * Base class for elastic search index gateways, with support for index creation and building stream sinks
  *
  * @param hosts      node addresses
  * @param indexName  name of the elasticsearch index
  * @param typeName   name of the elasticsearch type
  */
abstract class ElasticSearchIndex(hosts: Seq[ElasticSearchNode], indexName: String, typeName: String) extends Serializable {
  def create(dropFirst: Boolean = true): Unit = {
    val client = createClient(hosts)
    try {
      if (dropFirst) utils.dropIndex(client, indexName)

      client.execute {
        createIndex(indexName)
          .mappings(
            mapping(typeName).fields(
              createFields())
          )
      }.await
    }
    finally client.close()
  }

  protected def createFields(): Iterable[FieldDefinition]

  private def createClient(hosts: Seq[ElasticSearchNode]) = ElasticClient(ElasticProperties(hosts.map(createEndpoint)))

  private def createEndpoint(node: ElasticSearchNode) = ElasticNodeEndpoint(node.scheme, node.hostname, node.port, None)
}

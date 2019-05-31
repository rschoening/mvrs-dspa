package org.mvrs.dspa.utils.elastic

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition

/**
  * Base class for elastic search index gateways, with support for index creation and building stream sinks
  *
  * @param nodes     node addresses
  * @param indexName name of the elasticsearch index
  */
abstract class ElasticSearchIndex(val indexName: String, nodes: ElasticSearchNode*) extends Serializable {
  val typeName = s"$indexName-type" // type name derived, now that ES supports only one type per index

  /**
    * Creates the index, optionally deleting it first.
    *
    * @param dropFirst indicates if an existing index should be deleted first. If false and the index exists,
    *                  the method succeeds without changing the index
    * @param shards    the number of shards (partitions)
    * @param replicas  the number of replicas (0 = no second replica, each shard stored on only one node)
    * @note if (replicas + 1) is larger than the number of nodes in the cluster, the index will appear
    *       with status 'yellow' (not fully replicated)
    */
  def create(dropFirst: Boolean = true, shards: Int = 5, replicas: Int = 1): Unit = {
    val client = createClient(nodes: _*)
    try {
      if (dropFirst) dropIndex(client, indexName)

      client.execute {
        createIndex(indexName)
          .shards(shards)
          .replicas(replicas)
          .mappings(
            mapping(typeName).fields(
              createFields())
          )
      }.await
    }
    finally client.close()
  }

  protected def createFields(): Iterable[FieldDefinition]
}

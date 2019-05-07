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

  def create(dropFirst: Boolean = true, shards: Int = 5): Unit = {
    val client = createClient(nodes: _*)
    try {
      if (dropFirst) dropIndex(client, indexName)

      client.execute {
        createIndex(indexName)
          .shards(shards)
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

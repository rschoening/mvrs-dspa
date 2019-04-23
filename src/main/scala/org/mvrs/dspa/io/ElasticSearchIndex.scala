package org.mvrs.dspa.io

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition


/**
  * Base class for elastic search index gateways, with support for index creation and building stream sinks
  *
  * @param nodes     node addresses
  * @param indexName name of the elasticsearch index
  * @param typeName  name of the elasticsearch type
  */
abstract class ElasticSearchIndex(val indexName: String, val typeName: String, nodes: ElasticSearchNode*) extends Serializable {
  def create(dropFirst: Boolean = true): Unit = {
    val client = ElasticSearchUtils.createClient(nodes: _*)
    try {
      if (dropFirst) ElasticSearchUtils.dropIndex(client, indexName)

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
}

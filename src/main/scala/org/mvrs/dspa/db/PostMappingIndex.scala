package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.PostMapping
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

/**
  * Index of known mappings of comment ids to post ids
  *
  * @param indexName Name of the ElasticSearch index
  * @param nodes     Node addresses
  */
class PostMappingIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[PostMapping](indexName, nodes: _*) {

  override protected def getDocumentId(record: PostMapping): String = record.commentId.toString

  override protected def createDocument(input: PostMapping): Map[String, Any] =
    Map(
      "postId" -> input.postId,
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Iterable(
      longField("postId")
    )
}

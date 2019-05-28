package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.model.PostInfo
import org.mvrs.dspa.utils.elastic.{ElasticSearchIndexSink, ElasticSearchNode}

class PostInfoIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[PostInfo](indexName, nodes: _*) {
  override protected def getDocumentId(record: PostInfo): String = record.postId.toString

  override protected def createDocument(record: PostInfo): Map[String, Any] =
    Map(
      "personId" -> record.personId,
      "forumId" -> record.forumId,
      "forumTitle" -> record.forumTitle,
      "content" -> record.content,
      "imageFile" -> record.imageFile,
      "timestamp" -> record.timestamp,
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Iterable(
      longField("personId"),
      longField("forumId"),
      keywordField("forumTitle"),
      textField("content"),
      textField("imageFile"),
      dateField("timestamp"),
    )
}

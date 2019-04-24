package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl.{longField, _}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexSink, ElasticSearchNode}

import scala.collection.JavaConverters._

class PostFeaturesIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[PostFeatures](indexName, nodes: _*) {
  override protected def getDocumentId(record: PostFeatures): String = record.postId.toString

  override protected def createDocument(record: PostFeatures): Map[String, Any] =
    Map[String, Any](
      "personId" -> record.personId,
      "forumId" -> record.forumId,
      "content" -> record.content,
      "imageFile" -> record.imageFile,
      "features" -> record.features.asJava,
      "timestamp" -> record.timestamp,
    )

  override protected def createFields(): Iterable[FieldDefinition] =
    Seq(
      longField("personId"),
      longField("forumId"), // TODO add forum title
      textField("content"),
      textField("imageFile"),
      textField("features").index(false),
      dateField("timestamp"),
    )
}

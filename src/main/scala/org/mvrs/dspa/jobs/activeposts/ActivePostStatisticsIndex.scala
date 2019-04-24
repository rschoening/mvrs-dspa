package org.mvrs.dspa.jobs.activeposts

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.io.{ElasticSearchIndexSink, ElasticSearchNode}

class ActivePostStatisticsIndex(indexName: String, typeName: String, nodes: ElasticSearchNode*) extends ElasticSearchIndexSink[PostStatistics](indexName, typeName, nodes: _*) {

  override protected def getDocumentId(record: PostStatistics): String = s"${record.postId}#${record.time}"

  override protected def createDocument(record: PostStatistics): Map[String, Any] =
    Map[String, Any](
      "postId" -> record.postId,
      "replyCount" -> record.replyCount,
      "commentCount" -> record.commentCount,
      "likeCount" -> record.likeCount,
      "distinctUserCount" -> record.distinctUserCount,
      "newPost" -> record.newPost,
      "timestamp" -> record.time
    )

  override protected def createFields(): Iterable[FieldDefinition] = {
    Iterable(
      longField("postId"),
      intField("replyCount").index(false),
      intField("likeCount").index(false),
      intField("commentCount").index(false),
      intField("distinctUserCount").index(false),
      booleanField("newPost").index(false),
      dateField("timestamp")
    )

  }
}

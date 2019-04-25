package org.mvrs.dspa.db

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.mvrs.dspa.elastic.{ElasticSearchIndexSink, ElasticSearchNode}
import org.mvrs.dspa.model.PostStatistics

class ActivePostStatisticsIndex(indexName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchIndexSink[(PostStatistics, String, String)](indexName, nodes: _*) {

  override protected def getDocumentId(record: (PostStatistics, String, String)): String = s"${record._1.postId}#${record._1.time}"

  override protected def createDocument(record: (PostStatistics, String, String)): Map[String, Any] =
    Map[String, Any](
      "postId" -> record._1.postId,
      "content" -> record._2,
      "forumTitle" -> record._3,
      "replyCount" -> record._1.replyCount,
      "commentCount" -> record._1.commentCount,
      "likeCount" -> record._1.likeCount,
      "distinctUserCount" -> record._1.distinctUserCount,
      "newPost" -> record._1.newPost,
      "timestamp" -> record._1.time
    )

  override protected def createFields(): Iterable[FieldDefinition] = {
    Iterable(
      longField("postId"),
      keywordField("content"),
      keywordField("forumTitle"),
      intField("replyCount").index(false),
      intField("likeCount").index(false),
      intField("commentCount").index(false),
      intField("distinctUserCount").index(false),
      booleanField("newPost").index(false),
      dateField("timestamp")
    )
  }
}

package org.mvrs.dspa.jobs.recommendations

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams, utils}

object PostFeaturesJob extends FlinkStreamingJob(parallelism = 4) {
  val postFeaturesIndexName = "recommendations_posts"
  val postFeaturesTypeName = "recommendations_posts_type"
  val forumFeaturesIndexName = "recommendation_forum_features"
  val elasticSearchNode = ElasticSearchNode("localhost")

  val esNodes = Settings.elasticSearchNodes()
  val postFeaturesIndex = new PostFeaturesIndex(postFeaturesIndexName, postFeaturesTypeName, esNodes: _*)
  postFeaturesIndex.create()

  // val postsStream = streams.posts(Some("post-features"))
  val postsStream = streams.posts()

  val postsWithForumFeatures = utils.asyncStream(
    postsStream, new AsyncForumLookupFunction(forumFeaturesIndexName, esNodes: _*))

  val postFeatures = postsWithForumFeatures.map(createPostRecord _)

  postFeatures.addSink(postFeaturesIndex.createSink(100))

  env.execute("post features")

  private def createPostRecord(t: (PostEvent, Set[String])): PostFeatures = {
    val postEvent = t._1
    val forumFeatures = t._2

    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId,
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse(""),
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, FeaturePrefix.Tag))
    )
  }
}

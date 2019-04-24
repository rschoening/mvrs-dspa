package org.mvrs.dspa.jobs.recommendations

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.{ElasticSearchIndexes, PostFeatures}
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams, utils}

object PostFeaturesJob extends FlinkStreamingJob(parallelism = 4) {
  ElasticSearchIndexes.postFeatures.create()

  // val postsStream = streams.posts(Some("post-features"))
  val postsStream = streams.posts()

  val postsWithForumFeatures =
    utils.asyncStream(
      postsStream,
      new AsyncForumLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        Settings.elasticSearchNodes: _*))

  val postFeatures = postsWithForumFeatures.map(createPostRecord _)

  postFeatures.addSink(ElasticSearchIndexes.postFeatures.createSink(100))

  env.execute("post features")

  private def createPostRecord(t: (PostEvent, Set[String])): PostFeatures = {
    val postEvent = t._1
    val forumFeatures = t._2

    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId, // TODO add forum name also
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse(""),
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, FeaturePrefix.Tag))
    )
  }
}

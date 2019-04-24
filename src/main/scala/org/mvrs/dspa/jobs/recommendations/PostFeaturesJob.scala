package org.mvrs.dspa.jobs.recommendations

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.{streams, utils}

object PostFeaturesJob extends App {
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  val postFeaturesIndexName = "recommendations_posts"
  val postFeaturesTypeName = "recommendations_posts_type"
  val forumFeaturesIndexName = "recommendation_forum_features"
  val elasticSearchNode = ElasticSearchNode("localhost")

  val postFeaturesIndex = new PostFeaturesIndex(postFeaturesIndexName, postFeaturesTypeName, elasticSearchNode)
  postFeaturesIndex.create()

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(4)

  // val postsStream = streams.posts(Some("post-features"))
  val postsStream = streams.posts()

  val postsWithForumFeatures = utils.asyncStream(
    postsStream, new AsyncForumLookupFunction(forumFeaturesIndexName, elasticSearchNode))

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

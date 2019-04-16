package org.mvrs.dspa.jobs.recommendations

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams}

object PostFeaturesJob extends App {
  val postFeaturesIndexName = "recommendations_posts"
  val postFeaturesTypeName = "recommendations_posts_type"
  val forumFeaturesIndexName = "recommendation_forum_features"
  val elasticSearchNode = ElasticSearchNode("localhost")

  val postFeaturesIndex = new PostFeaturesIndex(postFeaturesIndexName, postFeaturesTypeName, elasticSearchNode)
  postFeaturesIndex.create()

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(5)

  val consumerGroup = "post-features"
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  //  val postsStream = streams.postsFromKafka(consumerGroup, speedupFactor, randomDelay)
  val postsStream = streams.postsFromCsv(Settings.postStreamCsvPath, speedupFactor, randomDelay)

  val postsWithForumFeatures = AsyncDataStream.unorderedWait(
    postsStream.javaStream,
    new AsyncForumLookup(forumFeaturesIndexName, elasticSearchNode),
    2000L, TimeUnit.MILLISECONDS,
    5)

  val featurePrefix = "T"

  val postFeatures =
    postsWithForumFeatures
      .map(createPostRecord(_, featurePrefix))
      .returns(createTypeInformation[PostFeatures])
  postFeatures.addSink(postFeaturesIndex.createSink(100))

  env.execute("post features")

  private def createPostRecord(t: (PostEvent, Set[String]), featurePrefix: String): PostFeatures = {
    val postEvent = t._1
    val forumFeatures = t._2

    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId,
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse(""),
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, featurePrefix))
    )
  }
}

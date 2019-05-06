package org.mvrs.dspa.jobs.recommendations

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.{PostEvent, PostFeatures}
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

// NOTE generic types have to be enabled when reading from Kafka. Otherwise:
// java.lang.UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig and type org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition is treated as a generic type.
object PostFeaturesJob extends FlinkStreamingJob(parallelism = 4, enableGenericTypes = true) {
  def execute(): Unit = {
    ElasticSearchIndexes.postFeatures.create()

    val kafkaConsumerGroup = Some("post-features") // None for csv
    val postsStream = streams.posts(kafkaConsumerGroup)

    val postsWithForumFeatures: DataStream[(PostEvent, String, Set[String])] = lookupForumFeatures(postsStream)

    val postFeatures = postsWithForumFeatures.map(createPostRecord _)

    postFeatures.addSink(ElasticSearchIndexes.postFeatures.createSink(100))

    env.execute("post features")
  }

  private def lookupForumFeatures(postsStream: DataStream[PostEvent]): DataStream[(PostEvent, String, Set[String])] = {
    FlinkUtils.asyncStream(
      postsStream,
      new AsyncForumLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        Settings.elasticSearchNodes: _*))
  }

  private def createPostRecord(t: (PostEvent, String, Set[String])): PostFeatures = {
    val postEvent = t._1
    val forumTitle = t._2
    val forumFeatures = t._3

    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId,
      forumTitle,
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse(""),
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, FeaturePrefix.Tag))
    )
  }
}

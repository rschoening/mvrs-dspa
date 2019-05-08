package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.jobs.recommendations.AsyncForumLookupFunction
import org.mvrs.dspa.model._
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

// TODO observe checkpoint size/time (growth?)
// TODO consider carrying forum id with post id, caching it (with LRU-based eviction and async db lookup in case of cache miss)
// TODO should post infos be processed in a separate stream, to avoid backpressure to source and hence slowing down also the statistics calculation?
//      --> separate kafka consumer group
//      --> maybe still in same job?
// NOTE: KafkaTopicPartition is treated as generic type, must enable generic types
object ActivePostStatisticsJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): Unit = {
    // read settings
    val windowSize = Settings.duration("jobs.active-post-statistics.window-size")
    val windowSlide = Settings.duration("jobs.active-post-statistics.window-slide")
    val countPostAuthor = Settings.config.getBoolean("jobs.active-post-statistics.count-post-author")
    val stateTtl = FlinkUtils.getTtl(windowSize, Settings.config.getInt("data.speedup-factor"))

    // (re)create elasticsearch index for post infos
    ElasticSearchIndexes.postInfos.create()

    // (re)create kafka topic for post statistics
    KafkaTopics.postStatistics.create(3, 1, overwrite = true)

    // consume events from kafka
    val kafkaConsumerGroup = Some("active-post-statistics") // None for csv
    val commentsStream = streams.comments(kafkaConsumerGroup)
    val postsStream = streams.posts(kafkaConsumerGroup)
    val likesStream = streams.likes(kafkaConsumerGroup)

    val postInfoStream = streams.posts(Some("active-post-statistics-postinfos"))

    // write post infos to elasticsearch, for lookup when writing post stats to elasticsearch
    lookupForumFeatures(postInfoStream)
      .addSink(ElasticSearchIndexes.postInfos.createSink(10))
      .name(s"elasticsearch: ${ElasticSearchIndexes.postInfos.indexName}")

    // calculate post statistics
    val statsStream = statisticsStream(
      commentsStream, postsStream, likesStream,
      windowSize, windowSlide, stateTtl, countPostAuthor)

    // write to kafka topic
    statsStream
      .keyBy(_.postId) // key by post id to preserve order
      .addSink(KafkaTopics.postStatistics.producer())
      .name(s"kafka: ${KafkaTopics.postStatistics.name}")

    env.execute("write post statistics to kafka (and post info to elasticsearch)")
  }

  //noinspection ConvertibleToMethodValue
  def statisticsStream(commentsStream: DataStream[CommentEvent],
                       postsStream: DataStream[PostEvent],
                       likesStream: DataStream[LikeEvent],
                       windowSize: Time, slide: Time, stateTtl: Time,
                       countPostAuthor: Boolean): DataStream[PostStatistics] = {
    val comments = commentsStream
      .map(createEvent(_)).name("map: event record")
      .keyBy(_.postId)

    val posts = postsStream
      .map(createEvent(_)).name("map: event record")
      .keyBy(_.postId)

    val likes = likesStream
      .map(createEvent(_)).name("map: event record")
      .keyBy(_.postId)

    posts
      .union(comments, likes)
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(windowSize, slide, stateTtl, countPostAuthor))
      .name(
        s"calculate post statistics " +
          s"(window: ${DateTimeUtils.formatDuration(windowSize.toMilliseconds)} / " +
          s"slide ${DateTimeUtils.formatDuration(slide.toMilliseconds)})"
      )
  }

  private def lookupForumFeatures(postsStream: DataStream[PostEvent]): DataStream[PostInfo] =
    FlinkUtils.asyncStream(
      postsStream,
      new AsyncForumLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        Settings.elasticSearchNodes: _*)).name("DB: look up forum title for post")
      .map(t => createPostInfo(t._1, t._2)).name("map: post info record")

  private def createPostInfo(postEvent: PostEvent, forumTitle: String): PostInfo =
    PostInfo(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId,
      forumTitle,
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse("")
    )

  private def createEvent(e: LikeEvent) = Event(EventType.Like, e.postId, e.personId, e.timestamp)

  private def createEvent(e: PostEvent) = Event(EventType.Post, e.postId, e.personId, e.timestamp)

  private def createEvent(e: CommentEvent) =
    Event(
      e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
      e.postId,
      e.personId,
      e.timestamp
    )
}





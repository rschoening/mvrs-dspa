package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model._
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

/**
  * Streaming job for writing post statistics to Kafka, and writing enriched posts (including forum title)
  * to ElasticSearch (DSPA Task #1)
  */
object ActivePostStatisticsJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): Unit = {
    // read settings
    val windowSize = Settings.duration("jobs.active-post-statistics.window-size")
    val windowSlide = Settings.duration("jobs.active-post-statistics.window-slide")
    val countPostAuthor = Settings.config.getBoolean("jobs.active-post-statistics.count-post-author")
    val batchSize = Settings.config.getInt("jobs.active-post-statistics.post-info-elasticsearch-batch-size")
    val stateTtl = FlinkUtils.getTtl(windowSize, Settings.config.getInt("data.speedup-factor"))
    val postInfoIndex = ElasticSearchIndexes.postInfos

    // implicits
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes

    // (re)create elasticsearch index for post infos
    postInfoIndex.create()

    // consume events from kafka
    val kafkaConsumerGroup = Some("active-post-statistics")
    val commentsStream = streams.comments(kafkaConsumerGroup)
    val postsStream = streams.posts(kafkaConsumerGroup)
    val likesStream = streams.likes(kafkaConsumerGroup)

    val postInfoStream = streams.posts(Some("active-post-statistics-postinfos")) // read using separate consumer group

    // write post infos to elasticsearch, for lookup when writing post stats to elasticsearch
    lookupForumFeatures(postInfoStream)
      .addSink(postInfoIndex.createSink(batchSize))
      .name(s"ElasticSearch: ${postInfoIndex.indexName}")

    // calculate post statistics
    val statsStream = statisticsStream(
      commentsStream, postsStream, likesStream,
      windowSize, windowSlide, stateTtl, countPostAuthor)

    // write to kafka topic (key by post id to preserve order)
    // TODO ensure that kafka partitioner picks up the key (KeyedSerializationSchema)
    FlinkUtils.writeToNewKafkaTopic(
      statsStream.keyBy(_.postId),
      KafkaTopics.postStatistics,
      Settings.config.getInt("data.kafka-partition-count"),
      None,
      Settings.config.getInt("data.kafka-replica-count").toShort,
      semantic = Semantic.EXACTLY_ONCE
    )

    FlinkUtils.printExecutionPlan()

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

  private def lookupForumFeatures(postsStream: DataStream[PostEvent])
                                 (implicit esNodes: Seq[ElasticSearchNode]): DataStream[PostInfo] =
    FlinkUtils.asyncStream(
      postsStream,
      new AsyncForumTitleLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        esNodes: _*)).name("Async I/O: look up forum title for post")
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





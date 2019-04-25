package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.{FlinkStreamingJob, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

object ActivePostStatisticsJob extends FlinkStreamingJob {
  val consumerGroup = "active-post-statistics"

  val commentsStream = streams.comments() // Some(consumerGroup))
  val postsStream = streams.posts() //Some(consumerGroup))
  val likesStream = streams.likes() //Option(consumerGroup))

  val statsStream = statisticsStream(
    commentsStream, postsStream, likesStream,
    Time.hours(12).toMilliseconds,
    Time.minutes(30).toMilliseconds)

  statsStream
    .keyBy(_.postId)
    .addSink(FlinkUtils.createKafkaProducer(
      "mvrs_poststatistics",
      Settings.config.getString("kafka.brokers"),
      createTypeInformation[PostStatistics]))

  env.execute("write post statistics to elastic search")

  //noinspection ConvertibleToMethodValue
  def statisticsStream(commentsStream: DataStream[CommentEvent],
                       postsStream: DataStream[PostEvent],
                       likesStream: DataStream[LikeEvent],
                       windowSize: Long,
                       slide: Long): DataStream[PostStatistics] = {
    val comments = commentsStream
      .map(createEvent(_))
      .keyBy(_.postId)

    val posts = postsStream
      .map(createEvent(_))
      .keyBy(_.postId)

    val likes = likesStream
      .map(createEvent(_))
      .keyBy(_.postId)

    posts
      .union(comments, likes)
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(windowSize, slide))
  }

  private def createEvent(e: LikeEvent) = Event(EventType.Like, e.postId, e.personId, e.timestamp)

  private def createEvent(e: PostEvent) = Event(EventType.Post, e.postId, e.personId, e.timestamp)

  private def createEvent(e: CommentEvent) = Event(
    e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
    e.postId, e.personId, e.timestamp)
}





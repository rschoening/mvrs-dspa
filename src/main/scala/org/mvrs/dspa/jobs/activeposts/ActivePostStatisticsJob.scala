package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

object ActivePostStatisticsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    val windowSize = Settings.duration("jobs.active-post-statistics.window-size")
    val windowSlide = Settings.duration("jobs.active-post-statistics.window-slide")
    val countPostAuthor = Settings.config.getBoolean("jobs.active-post-statistics.count-post-author")
    val stateTtl = FlinkUtils.getTtl(windowSize, Settings.config.getInt("data.speedup-factor"))

    val consumerGroup = "active-post-statistics"
    val commentsStream = streams.comments() // Some(consumerGroup))
    val postsStream = streams.posts() //Some(consumerGroup))
    val likesStream = streams.likes() //Option(consumerGroup))

    val statsStream = statisticsStream(
      commentsStream, postsStream, likesStream,
      windowSize, windowSlide, stateTtl, countPostAuthor)

    statsStream
      .keyBy(_.postId)
      .addSink(FlinkUtils.createKafkaProducer[PostStatistics](
        "mvrs_poststatistics",
        Settings.config.getString("kafka.brokers")))

    env.execute("write post statistics to elastic search")
  }

  //noinspection ConvertibleToMethodValue
  def statisticsStream(commentsStream: DataStream[CommentEvent],
                       postsStream: DataStream[PostEvent],
                       likesStream: DataStream[LikeEvent],
                       windowSize: Time, slide: Time, stateTtl: Time,
                       countPostAuthor: Boolean): DataStream[PostStatistics] = {
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
      .process(new PostStatisticsFunction(windowSize, slide, stateTtl, countPostAuthor))
  }

  private def createEvent(e: LikeEvent) = Event(EventType.Like, e.postId, e.personId, e.timestamp)

  private def createEvent(e: PostEvent) = Event(EventType.Post, e.postId, e.personId, e.timestamp)

  private def createEvent(e: CommentEvent) = Event(
    e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
    e.postId, e.personId, e.timestamp)
}





package org.mvrs.dspa.activeposts

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.activeposts.EventType.EventType
import org.mvrs.dspa.events.{CommentEvent, LikeEvent, PostEvent, PostStatistics}
import org.mvrs.dspa.functions.ScaledReplayFunction
import org.mvrs.dspa.utils

object ActivePostStatistics extends App {
  val props = new Properties()
  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("group.id", "test")
  props.setProperty("isolation.level", "read_committed")

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(3)

  val commentsSource = utils.createKafkaConsumer("comments", createTypeInformation[CommentEvent], props)
  val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent], props)
  val likesSource = utils.createKafkaConsumer("likes", createTypeInformation[LikeEvent], props)

  val speedupFactor = 1000
  val randomDelay = 0 // TODO input or scaled time?
  val maxOutOfOrderness = Time.milliseconds(randomDelay)

  val commentsStream: DataStream[CommentEvent] = env
    .addSource(commentsSource)
    .process(new ScaledReplayFunction[CommentEvent](_.timeStamp, speedupFactor, randomDelay))

  val postsStream: DataStream[PostEvent] = env
    .addSource(postsSource)
    .process(new ScaledReplayFunction[PostEvent](_.timeStamp, speedupFactor, randomDelay))

  val likesStream: DataStream[LikeEvent] = env
    .addSource(likesSource)
    .process(new ScaledReplayFunction[LikeEvent](_.timeStamp, speedupFactor, randomDelay))

  val stats = statistics(
    commentsStream, postsStream, likesStream,
    Time.hours(12).toMilliseconds,
    Time.minutes(30).toMilliseconds)

  stats.print

  def statistics(commentsStream: DataStream[CommentEvent],
                 postsStream: DataStream[PostEvent],
                 likesStream: DataStream[LikeEvent],
                 windowSize: Long,
                 slide: Long): DataStream[PostStatistics] = {
    val comments: KeyedStream[Event, Long] = commentsStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timeStamp))
      .map(e => Event(e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
        e.postId, e.personId, e.timeStamp))
      .keyBy(_.postId)

    val posts: KeyedStream[Event, Long] = postsStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timeStamp))
      .map(e => Event(EventType.Post, e.id, e.personId, e.timeStamp))
      .keyBy(_.postId)

    val likes: KeyedStream[Event, Long] = likesStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timeStamp))
      .map(e => Event(EventType.Like, e.postId, e.personId, e.timeStamp))
      .keyBy(_.postId)

    posts
      .union(comments, likes)
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(windowSize, slide))
  }
}

object EventType extends Enumeration {
  type EventType = Value
  val Post, Comment, Reply, Like = Value
}

case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)

package org.mvrs.dspa.activeposts

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.activeposts.EventType.EventType
import org.mvrs.dspa.events.{CommentEvent, LikeEvent, PostEvent, PostStatistics}
import org.mvrs.dspa.{streams, utils}

object ActivePostStatisticsJob extends App {

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(3)

  val consumerGroup = "activePostStatistics"
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  val commentsStream = streams.comments(consumerGroup, speedupFactor, randomDelay)
  val postsStream = streams.posts(consumerGroup, speedupFactor, randomDelay)
  val likesStream = streams.likes(consumerGroup, speedupFactor, randomDelay)

  val statsStream = statisticsStream(
    commentsStream, postsStream, likesStream,
    Time.hours(12).toMilliseconds,
    Time.minutes(30).toMilliseconds)

  statsStream
    .keyBy(_.postId)
    .addSink(utils.createKafkaProducer("poststatistics", utils.kafkaBrokers, createTypeInformation[PostStatistics]))

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

  private def createEvent(e: LikeEvent) = Event(EventType.Like, e.postId, e.personId, e.creationDate)

  private def createEvent(e: PostEvent) = Event(EventType.Post, e.id, e.personId, e.creationDate)

  private def createEvent(e: CommentEvent) = Event(
    e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
    e.postId, e.personId, e.creationDate)
}

object EventType extends Enumeration {
  type EventType = Value
  val Post, Comment, Reply, Like = Value
}

case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)


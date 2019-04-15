package org.mvrs.dspa

import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.events.{CommentEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.functions.ScaledReplayFunction

package object streams {
  val kafkaBrokers = "localhost:9092"

  def comments(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    val commentsSource = utils.createKafkaConsumer("comments", createTypeInformation[CommentEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(commentsSource)
      .keyBy(_.commentId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, CommentEvent](_.timestamp, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timestamp))
  }

  def posts(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(postsSource)
      .keyBy(_.postId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, PostEvent](_.timestamp, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timestamp))
  }

  def likes(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    val likesSource = utils.createKafkaConsumer("likes", createTypeInformation[LikeEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(likesSource)
      .keyBy(_.postId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, LikeEvent](_.timestamp, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timestamp))
  }

  private def getKafkaConsumerProperties(consumerGroup: String) = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", consumerGroup)
    props.setProperty("isolation.level", "read_committed")

    props
  }

  private def getMaxOutOfOrderness(speedupFactor: Int, randomDelay: Int) = Time.milliseconds(if (speedupFactor == 0) randomDelay else randomDelay / speedupFactor)
}

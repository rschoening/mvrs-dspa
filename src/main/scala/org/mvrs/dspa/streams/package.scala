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
      .process(new ScaledReplayFunction[CommentEvent](_.creationDate, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.creationDate))
  }

  def posts(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(postsSource)
      .process(new ScaledReplayFunction[PostEvent](_.creationDate, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.creationDate))
  }

  private def getKafkaConsumerProperties(consumerGroup: String) = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", consumerGroup)
    props.setProperty("isolation.level", "read_committed")

    props
  }

  private def getMaxOutOfOrderness(speedupFactor: Int, randomDelay: Int) = Time.milliseconds(if (speedupFactor == 0) randomDelay else randomDelay / speedupFactor)

  def likes(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    val likesSource = utils.createKafkaConsumer("likes", createTypeInformation[LikeEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(likesSource)
      .process(new ScaledReplayFunction[LikeEvent](_.creationDate, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.creationDate))
  }
}

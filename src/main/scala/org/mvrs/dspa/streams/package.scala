package org.mvrs.dspa

import kantan.csv.RowDecoder
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, SimpleScaledReplayFunction}
import org.mvrs.dspa.model.{CommentEvent, LikeEvent, PostEvent, RawCommentEvent}
import org.mvrs.dspa.utils.FlinkUtils

package object streams {
  def resolveReplyTree(rawComments: DataStream[RawCommentEvent]): DataStream[CommentEvent] =
    resolveReplyTree(rawComments, droppedRepliesStream = false)._1

  def resolveReplyTree(rawComments: DataStream[RawCommentEvent],
                       droppedRepliesStream: Boolean): (DataStream[CommentEvent], DataStream[RawCommentEvent]) = {
    val firstLevelComments =
      rawComments
        .filter(_.replyToPostId.isDefined).name("filter: first-level comments")
        .map(c =>
          CommentEvent(
            c.commentId,
            c.personId,
            c.creationDate,
            c.locationIP,
            c.browserUsed,
            c.content,
            c.replyToPostId.get,
            None,
            c.placeId
          )
        )
        .keyBy(_.postId)

    val repliesBroadcast =
      rawComments
        .filter(_.replyToPostId.isEmpty).name("filter: replies")
        .broadcast()

    val outputTagDroppedReplies = new OutputTag[RawCommentEvent]("dropped replies")

    val outputTag = if (droppedRepliesStream) Some(outputTagDroppedReplies) else None

    val rootedComments: DataStream[CommentEvent] = firstLevelComments
      .connect(repliesBroadcast)
      .process(new BuildReplyTreeProcessFunction(outputTag)).name("reconstruct reply tree")

    val droppedReplies = rootedComments.getSideOutput(outputTagDroppedReplies)

    (rootedComments, droppedReplies)
  }

  def comments(kafkaConsumerGroup: Option[String] = None, speedupFactorOverride: Option[Double] = None)
              (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    kafkaConsumerGroup.map(
      commentsFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride)
      )
    ).getOrElse(
      commentsFromCsv(
        Settings.config.getString("data.comments-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def posts(kafkaConsumerGroup: Option[String] = None, speedupFactorOverride: Option[Double] = None)
           (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] =
    kafkaConsumerGroup.map(
      postsFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride),
      )
    ).getOrElse(
      postsFromCsv(
        Settings.config.getString("data.posts-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def likes(kafkaConsumerGroup: Option[String] = None, speedupFactorOverride: Option[Double] = None)
           (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] =
    kafkaConsumerGroup.map(
      likesFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride),
      )
    ).getOrElse(
      likesFromCsv(
        Settings.config.getString("data.likes-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def rawCommentsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                        (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[RawCommentEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval)).name(s"$filePath")
  }

  def commentsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                     (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    resolveReplyTree(rawCommentsFromCsv(filePath, speedupFactor, randomDelay, watermarkInterval))
  }

  def likesFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[LikeEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval)).name(s"$filePath")
  }

  def postsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    implicit val decoder: RowDecoder[PostEvent] = PostEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[PostEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval)).name(s"$filePath")
  }

  def commentsFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                       (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    val topic = KafkaTopics.comments
    val commentsSource = topic.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(commentsSource).name(s"kafka: ${topic.name}")
      .map(new SimpleScaledReplayFunction[CommentEvent](_.timestamp, speedupFactor)).name("replay speedup")
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timestamp))
  }

  def postsFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    val topic = KafkaTopics.posts
    val postsSource = topic.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(postsSource).name(s"kafka: ${topic.name}")
      .map(new SimpleScaledReplayFunction[PostEvent](_.timestamp, speedupFactor)).name("replay speedup")
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timestamp))
  }

  def likesFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    val topic = KafkaTopics.likes
    val likesSource = topic.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(likesSource).name(s"kafka: ${topic.name}")
      .map(new SimpleScaledReplayFunction[LikeEvent](_.timestamp, speedupFactor)).name("replay speedup")
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timestamp))
  }

  private def getSpeedupFactor(speedupFactorOverride: Option[Double]): Double =
    speedupFactorOverride.getOrElse(Settings.config.getDouble("data.speedup-factor"))


  private def getMaxOutOfOrderness(speedupFactor: Double, randomDelay: Int): Time =
    Time.milliseconds(if (speedupFactor == 0.0) randomDelay else (randomDelay / speedupFactor).ceil.toLong)
}

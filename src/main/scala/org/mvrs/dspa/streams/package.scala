package org.mvrs.dspa

import kantan.csv.RowDecoder
import org.apache.flink.api.common.time
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, ScaledReplayFunction}
import org.mvrs.dspa.jobs.preparation.BuildReplyTreeProcessFunction
import org.mvrs.dspa.model.{CommentEvent, LikeEvent, PostEvent, RawCommentEvent}
import org.mvrs.dspa.utils.FlinkUtils

package object streams {
  def resolveReplyTree(rawComments: DataStream[RawCommentEvent]): DataStream[CommentEvent] =
    resolveReplyTree(rawComments, droppedRepliesStream = false)._1

  def resolveReplyTree(rawComments: DataStream[RawCommentEvent],
                       droppedRepliesStream: Boolean): (DataStream[CommentEvent], DataStream[RawCommentEvent]) = {
    val firstLevelComments =
      rawComments
        .filter(_.replyToPostId.isDefined)
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
        .filter(_.replyToPostId.isEmpty)
        .broadcast()

    val outputTagDroppedReplies = new OutputTag[RawCommentEvent]("dropped replies")

    val outputTag = if (droppedRepliesStream) Some(outputTagDroppedReplies) else None

    val rootedComments: DataStream[CommentEvent] = firstLevelComments
      .connect(repliesBroadcast)
      .process(new BuildReplyTreeProcessFunction(outputTag)).name("tree")

    val droppedReplies = rootedComments.getSideOutput(outputTagDroppedReplies)

    (rootedComments, droppedReplies)
  }

  def rawComments()(implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[RawCommentEvent](
        Settings.config.getString("data.comments-csv-path"),
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval")
      )
    )
  }

  def rawCommentsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                        (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[RawCommentEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def comments(kafkaConsumerGroup: Option[String] = None)(implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    kafkaConsumerGroup.map(
      commentsFromKafka(
        _,
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
      )
    ).getOrElse(
      commentsFromCsv(
        Settings.config.getString("data.comments-csv-path"),
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def posts(kafkaConsumerGroup: Option[String] = None)(implicit env: StreamExecutionEnvironment): DataStream[PostEvent] =
    kafkaConsumerGroup.map(
      postsFromKafka(
        _,
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
      )
    ).getOrElse(
      postsFromCsv(
        Settings.config.getString("data.posts-csv-path"),
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def likes(kafkaConsumerGroup: Option[String] = None)(implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] =
    kafkaConsumerGroup.map(
      likesFromKafka(
        _,
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
      )
    ).getOrElse(
      likesFromCsv(
        Settings.config.getString("data.likes-csv-path"),
        Settings.config.getInt("data.speedup-factor"),
        Settings.config.getInt("data.random-delay"),
        Settings.config.getInt("data.csv-watermark-interval"),
      )
    )

  def commentsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                     (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    resolveReplyTree(rawCommentsFromCsv(filePath, speedupFactor, randomDelay, watermarkInterval))
  }

  def likesFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[LikeEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def postsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    implicit val decoder: RowDecoder[PostEvent] = PostEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[PostEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def commentsFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)
                       (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    val commentsSource = KafkaTopics.comments.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(commentsSource)
      .keyBy(_.commentId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, CommentEvent](_.timestamp, speedupFactor, randomDelay, false))
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timestamp))
  }

  def postsFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    val postsSource = KafkaTopics.posts.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(postsSource)
      .keyBy(_.postId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, PostEvent](_.timestamp, speedupFactor, randomDelay, false))
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timestamp))
  }

  def likesFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    val likesSource = KafkaTopics.likes.consumer(consumerGroup)
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(likesSource)
      .keyBy(_.postId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, LikeEvent](_.timestamp, speedupFactor, randomDelay, false))
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timestamp))
  }

  private def getMaxOutOfOrderness(speedupFactor: Int, randomDelay: Int): time.Time =
    org.apache.flink.api.common.time.Time.milliseconds(if (speedupFactor == 0) randomDelay else randomDelay / speedupFactor)
}

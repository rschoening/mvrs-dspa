package org.mvrs.dspa

import kantan.csv.RowDecoder
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, SimpleScaledReplayFunction}
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.kafka.KafkaTopic

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
        randomDelay,
        csvWatermarkInterval,
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
        randomDelay,
        csvWatermarkInterval,
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
        randomDelay,
        csvWatermarkInterval,
      )
    )

  def postStatistics(kafkaConsumerGroup: String, speedupFactorOverride: Option[Double] = None)
                    (implicit env: StreamExecutionEnvironment): DataStream[PostStatistics] =
    postStatisticsFromKafka(kafkaConsumerGroup, getSpeedupFactor(speedupFactorOverride))


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
                       (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    fromKafka(KafkaTopics.comments, consumerGroup, _.timestamp, speedupFactor, randomDelay)

  def postStatisticsFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                             (implicit env: StreamExecutionEnvironment): DataStream[PostStatistics] =
    fromKafka(KafkaTopics.postStatistics, consumerGroup, _.time, speedupFactor, randomDelay)

  def postsFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] =
    fromKafka(KafkaTopics.posts, consumerGroup, _.timestamp, speedupFactor, randomDelay)

  def likesFromKafka(consumerGroup: String, speedupFactor: Double = 0, randomDelay: Int = 0)
                    (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] =
    fromKafka(KafkaTopics.likes, consumerGroup, _.timestamp, speedupFactor, randomDelay)

  private def fromKafka[T: TypeInformation](topic: KafkaTopic[T],
                                            consumerGroup: String,
                                            extractTime: T => Long,
                                            speedupFactor: Double,
                                            randomDelay: Int)
                                           (implicit env: StreamExecutionEnvironment): DataStream[T] = {
    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    val consumer = topic.consumer(consumerGroup)

    env.addSource(consumer)
      .name(s"kafka: ${topic.name}")
      .map(new SimpleScaledReplayFunction[T](extractTime, speedupFactor))
      .name("replay speedup")
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[T](maxOutOfOrderness, extractTime))
  }

  private def getSpeedupFactor(speedupFactorOverride: Option[Double]): Double =
    speedupFactorOverride.getOrElse(Settings.config.getDouble("data.speedup-factor"))

  private def csvWatermarkInterval: Int = Settings.config.getInt("data.csv-watermark-interval")

  private def randomDelay = Settings.config.getInt("data.random-delay")

  private def getMaxOutOfOrderness(speedupFactor: Double, randomDelay: Int): Time =
    Time.milliseconds(if (speedupFactor == 0.0) randomDelay else (randomDelay / speedupFactor).ceil.toLong)
}

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

  /**
    * Gets the stream of comments (with assigned post ids), either from kafka or from csv file
    *
    * @param kafkaConsumerGroup    if specified, the stream is consumed from kafka. Otherwise (None), from csv file
    * @param speedupFactorOverride optional override of the speedup factor defined in the settings
    * @param env                   the implicit stream execution environment
    * @return stream of comments with assigned post ids, i.e. after reply tree reconstruction
    */
  def comments(kafkaConsumerGroup: Option[String] = None, speedupFactorOverride: Option[Double] = None)
              (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    kafkaConsumerGroup.map(
      commentsFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride)
        // TODO outoforderness
      )
    ).getOrElse(
      commentsFromCsv(
        Settings.config.getString("data.comments-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        randomDelay,
        csvWatermarkInterval,
      )
    )

  /**
    * Gets the stream of post events, either from kafka or from csv file
    *
    * @param kafkaConsumerGroup    if specified, the stream is consumed from kafka. Otherwise (None), from csv file
    * @param speedupFactorOverride optional override of the speedup factor defined in the settings
    * @param env                   the implicit stream execution environment
    * @return stream of post events
    */
  def posts(kafkaConsumerGroup: Option[String] = None,
            speedupFactorOverride: Option[Double] = None)
           (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] =
    kafkaConsumerGroup.map(
      postsFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride),
        // TODO outoforderness
      )
    ).getOrElse(
      postsFromCsv(
        Settings.config.getString("data.posts-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        randomDelay,
        csvWatermarkInterval,
      )
    )

  /**
    * Gets the stream of like events, either from kafka or from csv file
    *
    * @param kafkaConsumerGroup    if specified, the stream is consumed from kafka. Otherwise (None), from csv file
    * @param speedupFactorOverride optional override of the speedup factor defined in the settings
    * @param env                   the implicit stream execution environment
    * @return stream of like events
    */
  def likes(kafkaConsumerGroup: Option[String] = None,
            speedupFactorOverride: Option[Double] = None)
           (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] =
    kafkaConsumerGroup.map(
      likesFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride),
        // TODO outoforderness
      )
    ).getOrElse(
      likesFromCsv(
        Settings.config.getString("data.likes-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        randomDelay,
        csvWatermarkInterval,
      )
    )

  /**
    * Gets the stream of post statistics from kafka
    *
    * @param kafkaConsumerGroup    kafka consumer group
    * @param speedupFactorOverride optional override of the speedup factor defined in the settings
    * @param env                   the implicit stream execution environment
    * @return stream of post statistics produced per active post at end of window
    */
  def postStatistics(kafkaConsumerGroup: String, speedupFactorOverride: Option[Double] = None)
                    (implicit env: StreamExecutionEnvironment): DataStream[PostStatistics] =
    postStatisticsFromKafka(kafkaConsumerGroup, getSpeedupFactor(speedupFactorOverride))

  def rawCommentsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Long = 0, watermarkInterval: Long = 10000)
                        (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[RawCommentEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay,
        watermarkInterval
      )
    ).name(s"$filePath")
  }

  def commentsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Long = 0, watermarkInterval: Long = 10000)
                     (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    resolveReplyTree(rawCommentsFromCsv(filePath, speedupFactor, randomDelay, watermarkInterval))
  }

  def likesFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Long = 0, watermarkInterval: Long = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[LikeEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay,
        watermarkInterval
      )
    ).name(s"$filePath")
  }

  def postsFromCsv(filePath: String, speedupFactor: Double = 0, randomDelay: Long = 0, watermarkInterval: Long = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    implicit val decoder: RowDecoder[PostEvent] = PostEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[PostEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay,
        watermarkInterval
      )
    ).name(s"$filePath (speedup: x $speedupFactor; randomDelay: $randomDelay)")
  }

  def commentsFromKafka(consumerGroup: String,
                        speedupFactor: Double = 0,
                        maxOutOfOrderness: Time = Time.milliseconds(0))
                       (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    fromKafka(KafkaTopics.comments, consumerGroup, _.timestamp, speedupFactor, maxOutOfOrderness)

  def postStatisticsFromKafka(consumerGroup: String,
                              speedupFactor: Double = 0,
                              maxOutOfOrderness: Time = Time.milliseconds(0))
                             (implicit env: StreamExecutionEnvironment): DataStream[PostStatistics] =
    fromKafka(KafkaTopics.postStatistics, consumerGroup, _.time, speedupFactor, maxOutOfOrderness)

  def postsFromKafka(consumerGroup: String,
                     speedupFactor: Double = 0,
                     maxOutOfOrderness: Time = Time.milliseconds(0))
                    (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] =
    fromKafka(KafkaTopics.posts, consumerGroup, _.timestamp, speedupFactor, maxOutOfOrderness)

  def likesFromKafka(consumerGroup: String,
                     speedupFactor: Double = 0,
                     maxOutOfOrderness: Time = Time.milliseconds(0))
                    (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] =
    fromKafka(KafkaTopics.likes, consumerGroup, _.timestamp, speedupFactor, maxOutOfOrderness)

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


  private def fromKafka[T: TypeInformation](topic: KafkaTopic[T],
                                            consumerGroup: String,
                                            extractTime: T => Long,
                                            speedupFactor: Double,
                                            maxOutOfOrderness: Time)
                                           (implicit env: StreamExecutionEnvironment): DataStream[T] = {

    val consumer = topic.consumer(consumerGroup)
    consumer.assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[T](maxOutOfOrderness, extractTime))

    env.addSource(consumer)
      .name(s"Kafka: ${topic.name}")
      .map(new SimpleScaledReplayFunction[T](extractTime, speedupFactor))
      .name(s"replay speedup (x $speedupFactor)")
  }

  private def getSpeedupFactor(speedupFactorOverride: Option[Double]): Double =
    speedupFactorOverride.getOrElse(Settings.config.getDouble("data.speedup-factor"))

  private def csvWatermarkInterval: Long = Settings.duration("data.csv-watermark-interval").toMilliseconds

  private def randomDelay = Settings.duration("data.random-delay").toMilliseconds

  // TODO revise
  private def getMaxOutOfOrderness(speedupFactor: Double, randomDelay: Long): Time =
    Time.milliseconds(if (speedupFactor == 0.0) randomDelay else (randomDelay / speedupFactor).ceil.toLong)
}

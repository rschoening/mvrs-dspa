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
  def rawComments(kafkaConsumerGroup: Option[String] = None, speedupFactorOverride: Option[Double] = None)
                 (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] =
    kafkaConsumerGroup.map(
      rawCommentsFromKafka(
        _,
        getSpeedupFactor(speedupFactorOverride),
        getMaxOutOfOrderness
      )
    ).getOrElse(
      rawCommentsFromCsv(
        Settings.config.getString("data.comments-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        getRandomDelay,
        csvWatermarkInterval,
      )
    )

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
        getSpeedupFactor(speedupFactorOverride),
        getMaxOutOfOrderness
      )
    ).getOrElse(
      commentsFromCsv(
        Settings.config.getString("data.comments-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        getRandomDelay,
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
        getMaxOutOfOrderness
      )
    ).getOrElse(
      postsFromCsv(
        Settings.config.getString("data.posts-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        getRandomDelay,
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
        getMaxOutOfOrderness
      )
    ).getOrElse(
      likesFromCsv(
        Settings.config.getString("data.likes-csv-path"),
        getSpeedupFactor(speedupFactorOverride),
        getRandomDelay,
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

  def rawCommentsFromCsv(filePath: String,
                         speedupFactor: Double = 0,
                         randomDelay: Time = Time.milliseconds(0),
                         watermarkInterval: Long = 10000)
                        (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[RawCommentEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay.toMilliseconds,
        watermarkInterval
      )
    ).name(s"$filePath")
  }

  def commentsFromCsv(filePath: String,
                      speedupFactor: Double = 0,
                      randomDelay: Time = Time.milliseconds(0),
                      watermarkInterval: Long = 10000)
                     (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    resolveReplyTree(rawCommentsFromCsv(filePath, speedupFactor, randomDelay, watermarkInterval))
  }

  def likesFromCsv(filePath: String,
                   speedupFactor: Double = 0,
                   randomDelay: Time = Time.milliseconds(0),
                   watermarkInterval: Long = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[LikeEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay.toMilliseconds,
        watermarkInterval
      )
    ).name(s"$filePath")
  }

  def postsFromCsv(filePath: String,
                   speedupFactor: Double = 0,
                   randomDelay: Time = Time.milliseconds(0),
                   watermarkInterval: Long = 10000)
                  (implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    implicit val decoder: RowDecoder[PostEvent] = PostEvent.csvDecoder

    env.addSource(
      new ReplayedCsvFileSourceFunction[PostEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor, // 0 -> unchanged read speed
        randomDelay.toMilliseconds,
        watermarkInterval
      )
    ).name(s"$filePath (speedup: x $speedupFactor; randomDelay: $randomDelay)")
  }

  def rawCommentsFromKafka(consumerGroup: String,
                           speedupFactor: Double = 0,
                           maxOutOfOrderness: Time = Time.milliseconds(0))
                          (implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] =
    fromKafka(KafkaTopics.comments, consumerGroup, _.timestamp, speedupFactor, maxOutOfOrderness)

  def commentsFromKafka(consumerGroup: String,
                        speedupFactor: Double = 0,
                        maxOutOfOrderness: Time = Time.milliseconds(0))
                       (implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] =
    resolveReplyTree(fromKafka(KafkaTopics.comments, consumerGroup, _.timestamp, speedupFactor, maxOutOfOrderness))

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

  /**
    * Transforms the stream of raw comments into comments with resolved reference to the post
    *
    * @param rawComments          the stream of raw comments consisting of first-level comments to posts, and replies to
    *                             either first-level comments or other replies. Replies don't have a direct reference to
    *                             the parent post.
    * @param droppedRepliesStream indicates that the dropped replies should be emitted on a side output stream. This is
    *                             useful to investigate the behavior on a single worker - with multiple workers, all
    *                             replies are dropped on all but one worker, which makes interpretation of this stream
    *                             more difficult.
    * @return the pair of comment stream, dropped reply stream (which may not emit elements depending on the value of
    *         { @code droppedRepliesStream}
    */
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
                                            maxOutOfOrderness: Time,
                                            readCommitted: Boolean = true)
                                           (implicit env: StreamExecutionEnvironment): DataStream[T] = {

    // NOTE: if watermark assigner is inserted BEFORE the SimpleScaledReplayFunction, then the AutoWatermarkInterval skips over
    //       the wait times, i.e. the clock stops during waits due to backpressure, with regard to this interval
    // An alternative would be to not block but use processing-time timers instead, however
    // 1) this would require the input stream to be keyed - at this point this should not be a requirement
    // 2) events would only be emitted at the arrival of watermarks (to trigger the timers), leading to an unexpectedly bursty stream
    //
    // On the other hand, assigning watermarks on the union of the per-partition streams can cause *late* events even if the
    // individual partitions were written in timestamp order, due to uneven/interleaved reading from the partitions.
    // This is especially notable with no replay scaling (speedup = 0).
    // -> for speedup = 0: assign watermarks per partition (on consumer)
    // -> for speedup > 0: assign watermarks after the scaled replay function, and make sure to use large-enough value for maxOutOfOrderness
    //                     OR use only one Kafka partition (for simulation purposes)
    // --> in both cases, the AutoWatermarkInterval can be interpreted as processing-time, independent of scaled-replay
    val consumer = topic.consumer(consumerGroup, readCommitted)

    val assigner = FlinkUtils.timeStampExtractor[T](maxOutOfOrderness, extractTime)

    if (speedupFactor == 0) consumer.assignTimestampsAndWatermarks(assigner)

    val stream = env.addSource(consumer).name(s"Kafka: ${topic.name}")

    if (speedupFactor == 0) stream
    else stream.map(new SimpleScaledReplayFunction[T](extractTime, speedupFactor))
      .name(s"replay speedup (x $speedupFactor)")
      .assignTimestampsAndWatermarks(assigner)
  }

  private def getSpeedupFactor(speedupFactorOverride: Option[Double]): Double =
    speedupFactorOverride.getOrElse(Settings.config.getDouble("data.speedup-factor"))

  private def csvWatermarkInterval: Long = Settings.duration("data.csv-watermark-interval").toMilliseconds

  private def getRandomDelay: Time = Time.milliseconds(Settings.duration("data.random-delay").toMilliseconds)

  private def getMaxOutOfOrderness: Time = Time.milliseconds(Settings.duration("data.max-out-of-orderness").toMilliseconds)
}

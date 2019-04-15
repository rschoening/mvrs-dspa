package org.mvrs.dspa

import java.util.Properties

import kantan.csv.RowDecoder
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.events.{CommentEvent, LikeEvent, PostEvent, RawCommentEvent}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, ScaledReplayFunction}
import org.mvrs.dspa.preparation.BuildReplyTreeProcessFunction

package object streams {
  val kafkaBrokers = "localhost:9092" // TODO to central configuration

  def resolveReplyTree(rawComments: DataStream[RawCommentEvent]): DataStream[CommentEvent] =
    resolveReplyTree(rawComments, droppedRepliesStream = false)._1

  def resolveReplyTree(rawComments: DataStream[RawCommentEvent], droppedRepliesStream: Boolean): (DataStream[CommentEvent], DataStream[RawCommentEvent]) = {
    val firstLevelComments = rawComments
      .filter(_.replyToPostId.isDefined)
      .map(c => CommentEvent(c.commentId, c.personId, c.creationDate, c.locationIP, c.browserUsed, c.content, c.replyToPostId.get, None, c.placeId))
      .keyBy(_.postId)

    val repliesBroadcast = rawComments
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

  def rawCommentsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)(implicit env: StreamExecutionEnvironment): DataStream[RawCommentEvent] = {
    implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[RawCommentEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def commentsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)(implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    resolveReplyTree(rawCommentsFromCsv(filePath, speedupFactor, randomDelay, watermarkInterval))
  }


  def likesFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)(implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
    implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[LikeEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def postsFromCsv(filePath: String, speedupFactor: Int = 0, randomDelay: Int = 0, watermarkInterval: Int = 10000)(implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    implicit val decoder: RowDecoder[PostEvent] = PostEvent.csvDecoder

    env.addSource(new ReplayedCsvFileSourceFunction[PostEvent](
      filePath,
      skipFirstLine = true, '|',
      extractEventTime = _.timestamp,
      speedupFactor, // 0 -> unchanged read speed
      randomDelay,
      watermarkInterval))
  }

  def commentsFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[CommentEvent] = {
    val commentsSource = utils.createKafkaConsumer("comments", createTypeInformation[CommentEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(commentsSource)
      .keyBy(_.commentId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, CommentEvent](_.timestamp, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timestamp))
  }

  def postsFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[PostEvent] = {
    val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent],
      getKafkaConsumerProperties(consumerGroup))

    val maxOutOfOrderness: Time = getMaxOutOfOrderness(speedupFactor, randomDelay)

    env.addSource(postsSource)
      .keyBy(_.postId) // only for scaled replay function (timer)
      .process(new ScaledReplayFunction[Long, PostEvent](_.timestamp, speedupFactor, randomDelay))
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timestamp))
  }

  def likesFromKafka(consumerGroup: String, speedupFactor: Int = 0, randomDelay: Int = 0)(implicit env: StreamExecutionEnvironment): DataStream[LikeEvent] = {
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

  private def getMaxOutOfOrderness(speedupFactor: Int, randomDelay: Int) =
    Time.milliseconds(if (speedupFactor == 0) randomDelay else randomDelay / speedupFactor)
}

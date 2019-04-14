package org.mvrs.dspa.preparation

import kantan.csv.RowDecoder
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, SimpleTextFileSinkFunction}
import org.mvrs.dspa.utils


object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"
  val speedupFactor = 0 // 10000

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(true) // use arg (scallop?)

  env.setParallelism(4)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // NOTE it seems that a non-default parallelism must be set for each operator, otherwise an immediate rebalance
  //      back to default parallelism occurs
  // NOTE if assignTimeStampsAndWatermarks is (accidentally) set to parallelism=1, then the monitor step
  //      (with parallelism 4) sees watermarks that area AHEAD (negative delays)

  //  import kantan.csv.refined._
  //  import kantan.csv.java8._
  //  implicit val decoder: RowDecoder[RawComment] = RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawComment.apply)
  //
  //  val rawComments =
  //    env.addSource(
  //      new ReplayedCsvFileSourceFunction[RawComment](
  //        filePath,
  //        skipFirstLine = true, '|',
  //        extractEventTime = _.timestamp,
  //        speedupFactor = speedupFactor, // 0 -> unchanged read speed
  //        maximumDelayMilliseconds = 10000,
  //        watermarkInterval = 10000))
  //  rawComments.print

  import kantan.csv.java8._
  implicit val decoder: RowDecoder[RawCommentEvent] = RowDecoder.decoder(0, 1, 2, 3, 4, 5, 6, 7, 8)(RawCommentEvent.apply)

  val allComments: DataStream[RawCommentEvent] =
    env.addSource(
      new ReplayedCsvFileSourceFunction[RawCommentEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor = speedupFactor, // 0 -> unchanged read speed
        maximumDelayMilliseconds = 10000,
        watermarkInterval = 10000))

  val (rootedComments, _) = resolveReplyTree(allComments, droppedRepliesStream = false)

  rootedComments.map(c => s"${c.commentId};-1;${c.postId};${c.creationDate}")
    .addSink(new SimpleTextFileSinkFunction("c:\\temp\\dspa_rooted"))

  // rootedComments.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[CommentEvent]))

  println(env.getExecutionPlan) // NOTE this is the same json as env.getStreamGraph.dumpStreamingPlanAsJSON()
  env.execute()

  def resolveReplyTree(rawComments: DataStream[RawCommentEvent]): (DataStream[CommentEvent], DataStream[RawCommentEvent]) = {
    resolveReplyTree(rawComments, droppedRepliesStream = true)
  }

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

}





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

  implicit val decoder: RowDecoder[RawCommentEvent] = RawCommentEvent.decoder

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





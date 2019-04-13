package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.functions.{ReplayedTextFileSourceFunction, SimpleTextFileSinkFunction}
import org.mvrs.dspa.preparation.LoadCommentEventsJob.ParseError
import org.mvrs.dspa.utils


object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"
  val speedupFactor = 10000

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(true) // use arg (scallop?)

  env.setParallelism(4)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val outputTagParsingErrors = new OutputTag[ParseError]("comment parsing errors")

  // NOTE it seems that a non-default parallelism must be set for each operator, otherwise an immediate rebalance
  //      back to default parallelism occurs
  // NOTE if assignTimeStampsAndWatermarks is (accidentally) set to parallelism=1, then the monitor step
  //      (with parallelism 4) sees watermarks that area AHEAD (negative delays)

  val allComments: DataStream[CommentEvent] =
    env.addSource(
      new ReplayedTextFileSourceFunction[CommentEvent](
        filePath,
        skipFirstLine = true,
        parse = CommentEvent.parse,
        extractEventTime = _.creationDate,
        speedupFactor = speedupFactor, // 0 -> unchanged read speed
        maximumDelayMilliseconds = 10000,
        watermarkInterval = 10000))

  val (rootedComments, _) = resolveReplyTree(allComments, droppedRepliesStream = false)

  rootedComments.map(c => s"${c.id};-1;${c.postId};${utils.formatTimestamp(c.creationDate)}")
    .addSink(new SimpleTextFileSinkFunction("c:\\temp\\dspa_rooted"))

  // rootedComments.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[CommentEvent]))

  println(env.getExecutionPlan) // NOTE this is the same json as env.getStreamGraph.dumpStreamingPlanAsJSON()
  env.execute()

  def resolveReplyTree(rawComments: DataStream[CommentEvent]): (DataStream[CommentEvent], DataStream[CommentEvent]) = {
    resolveReplyTree(rawComments, droppedRepliesStream = true)
  }

  def resolveReplyTree(rawComments: DataStream[CommentEvent], droppedRepliesStream: Boolean): (DataStream[CommentEvent], DataStream[CommentEvent]) = {
    val firstLevelComments = rawComments
      .filter(_.replyToPostId.isDefined)
      .keyBy(_.postId)

    val repliesBroadcast = rawComments
      .filter(_.replyToPostId.isEmpty)
      .broadcast()

    val outputTagDroppedReplies = new OutputTag[CommentEvent]("dropped replies")

    val outputTag = if (droppedRepliesStream) Some(outputTagDroppedReplies) else None

    val rootedComments: DataStream[CommentEvent] = firstLevelComments
      .connect(repliesBroadcast)
      .process(new BuildReplyTreeProcessFunction(outputTag)).name("tree")

    val droppedReplies = rootedComments.getSideOutput(outputTagDroppedReplies)

    (rootedComments, droppedReplies)
  }

}





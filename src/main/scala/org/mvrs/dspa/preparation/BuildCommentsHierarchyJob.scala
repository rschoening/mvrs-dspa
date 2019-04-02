package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.functions.{ProgressMonitorFunction, ScaledReplayFunction}
import org.mvrs.dspa.preparation.LoadCommentEventsJob.ParseError
import org.mvrs.dspa.utils

object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1) // NOTE with multiple workers, the comments AND broadcast stream watermarks lag VERY much behind
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(10L) // NOTE this is REQUIRED for timers to fire, apparently

  val outputTagParsingErrors = new OutputTag[ParseError]("comment parsing errors")

  // NOTE it seems that a non-default parallelism must be set for each operator, otherwise an immediate rebalance
  //      back to default parallelism occurs
  // NOTE if assignTimeStampsAndWatermarks is (accidentally) set to parallelism=1, then the monitor step
  //      (with parallelism 4) sees watermarks that area AHEAD (negative delays)

  val allComments: DataStream[CommentEvent] = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line? use table api csv source and convert to datastream?
    .map(CommentEvent.parse _) // TODO use parser process function with side output for errors
    .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](Time.milliseconds(1), _.creationDate))
    .process(new ScaledReplayFunction[CommentEvent](_.creationDate, 100000, 0))

  val (rootedComments, droppedReplies) = resolveReplyTree(allComments)

  rootedComments.process(new ProgressMonitorFunction[CommentEvent]("TREE", 1000)).name("tree_monitor")
  droppedReplies.process(new ProgressMonitorFunction[CommentEvent]("DROPPED", 1000))

  println(env.getExecutionPlan)
  env.execute()

  def resolveReplyTree(rawComments: DataStream[CommentEvent]): (DataStream[CommentEvent], DataStream[CommentEvent]) = {
    val postComments = rawComments
      .filter(_.replyToPostId.isDefined)
      // .process(new ProgressMonitorFunction[CommentEvent]("L1C", 1000))
      .keyBy(_.postId)

    val repliesBroadcast = rawComments
      .filter(_.replyToPostId.isEmpty)
      // .process(new ProgressMonitorFunction[CommentEvent]("REPLY", 1000))
      .broadcast()

    val outputTagDroppedReplies = new OutputTag[CommentEvent]("dropped replies")

    val rootedComments: DataStream[CommentEvent] = postComments
      .connect(repliesBroadcast)
      .process(new BuildReplyTreeProcessFunction(outputTagDroppedReplies)).name("tree")

    val droppedReplies = rootedComments
      .getSideOutput(outputTagDroppedReplies)

    (rootedComments, droppedReplies)
  }
}



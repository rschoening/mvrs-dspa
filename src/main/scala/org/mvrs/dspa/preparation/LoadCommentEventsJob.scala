package org.mvrs.dspa.preparation

import kantan.csv.RowDecoder
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.functions.{ReplayedCsvFileSourceFunction, SimpleTextFileSinkFunction}
import org.mvrs.dspa.preparation.BuildCommentsHierarchyJob.resolveReplyTree
import org.mvrs.dspa.utils


object LoadCommentEventsJob extends App {
  require(args.length == 1, "full path to csv file expected")

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

  // execute program
  env.execute("Import comment events from csv file to Kafka")

  //  case class ParseError(error: String, text: String)
  //
  //  class ParseCommentFunction extends KeyedProcessFunction[Int, String, CommentEvent] {
  //    private var postForComment: MapState[Long, Long] = _
  //
  //    override def open(parameters: Configuration): Unit = {
  //      postForComment = getRuntimeContext.getMapState(
  //        new MapStateDescriptor[Long, Long](
  //          "postForComment",
  //          createTypeInformation[Long],
  //          createTypeInformation[Long])
  //      )
  //    }
  //
  //    override def processElement(value: String,
  //                                ctx: KeyedProcessFunction[Int, String, CommentEvent]#Context,
  //                                out: Collector[CommentEvent]): Unit = {
  //      Try(CommentEvent.parse(value)).flatMap(
  //        c =>
  //          if (c.replyToPostId.isEmpty && c.replyToCommentId.isEmpty) Failure(new Exception("missing parent reference"))
  //          else if (c.replyToPostId.isEmpty) {
  //            val replyToCommentId = c.replyToCommentId.get
  //            if (!postForComment.contains(replyToCommentId)) Failure(new Exception(s"unknown comment: $replyToCommentId"))
  //            else Success(c.copy(replyToPostId = Some(postForComment.get(replyToCommentId))))
  //          } else {
  //            postForComment.put(c.id, c.replyToPostId.get)
  //            Success(c)
  //          }) match {
  //        case Success(c) => out.collect(c)
  //        case Failure(e) => ctx.output(outputTag, ParseError(e.getMessage, value))
  //      }
  //    }
  //  }

}


package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.utils

import scala.util.{Failure, Success, Try}


object LoadCommentEvents extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1) // do data preparation in one worker only

  val outputTag = new OutputTag[ParseError]("comment parsing errors")

  val stream = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line?
    .keyBy(_ => 0) // has to be keyed for map state to be available - or we could use operator state, or a simple hashmap (no checkpoints)
    .process(new ParseCommentFunction())

  val errorStream = stream.getSideOutput(outputTag)
  errorStream.print

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[CommentEvent]))

  // execute program
  env.execute("Import comment events from csv file to Kafka")

  case class ParseError(error: String, text: String)

  class ParseCommentFunction extends KeyedProcessFunction[Int, String, CommentEvent] {
    private var postForComment: MapState[Long, Long] = _

    override def open(parameters: Configuration): Unit = {
      postForComment = getRuntimeContext.getMapState(
        new MapStateDescriptor[Long, Long](
          "postForComment",
          createTypeInformation[Long],
          createTypeInformation[Long])
      )
    }

    override def processElement(value: String,
                                ctx: KeyedProcessFunction[Int, String, CommentEvent]#Context,
                                out: Collector[CommentEvent]): Unit = {
      Try(CommentEvent.parse(value)) match {
        case Success(comment) =>
          if (comment.replyToPostId.isEmpty) {
            if (comment.replyToCommentId.isEmpty) {
              ctx.output(outputTag, ParseError("missing parent reference", value))
            }
            else {
              val replyToCommentId = comment.replyToCommentId.get

              if (!postForComment.contains(replyToCommentId))
                ctx.output(outputTag, ParseError(s"unknown comment: $replyToCommentId", value))
              else
                out.collect(comment.copy(replyToPostId = Some(postForComment.get(replyToCommentId))))
            }
          }
          else {
            postForComment.put(comment.id, comment.replyToPostId.get)
            out.collect(comment)
          }
        case Failure(exception) => ctx.output(outputTag, ParseError(exception.getMessage, value))
      }
    }
  }

}


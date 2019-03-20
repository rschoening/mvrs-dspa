package org.mvrs.dspa.preparation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.utils


object LoadCommentEvents extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1) // do data preparation in one worker only

  val stream = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line?
    .keyBy(_ => 0) // has to be keyed for map state to be available - or we could use operator state, or a simple hashmap (no checkpoints)
    .map(new ParseComment())

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[CommentEvent]))

  // execute program
  env.execute("Import comment events from csv file to Kafka")

  class ParseComment extends RichMapFunction[String, CommentEvent] {
    private var postForComment: MapState[Long, Long] = _

    override def open(parameters: Configuration): Unit = {
      postForComment = getRuntimeContext.getMapState(
        new MapStateDescriptor[Long, Long](
          "postForComment",
          createTypeInformation[Long],
          createTypeInformation[Long])
      )
    }

    override def map(value: String): CommentEvent = {
      val comment = CommentEvent.parse(value)
      // 1045500

      if (comment.replyToPostId.isEmpty) {
        // fails if both ids are missing, or if post for replied-to comment is not known
        // TODO: use ProcessFunction instead of RichMapFunction, to emit data errors to Side Output stream?
        val postId = comment.replyToCommentId.map(id => postForComment.get(id)).get

        comment.copy(replyToPostId = Some(postId)) // never mind the copy, this is only data prep
      }
      else {
        postForComment.put(comment.id, comment.replyToPostId.get)
        comment
      }
    }
  }

}


package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.functions.SimpleTextFileSinkFunction
import org.mvrs.dspa.{streams, utils}


object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"
  val speedupFactor = 0 // 10000
  val maximumDelayMilliseconds = 10000
  val watermarkInterval = 10000

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(true) // use arg (scallop?)
  env.setParallelism(4)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val comments = streams.commentsFromCsv(filePath, speedupFactor, maximumDelayMilliseconds, watermarkInterval)

  comments.map(c => s"${c.commentId};-1;${c.postId};${c.creationDate}")
    .addSink(new SimpleTextFileSinkFunction("c:\\temp\\dspa_rooted"))

  // rootedComments.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[CommentEvent]))

  println(env.getExecutionPlan) // NOTE this is the same json as env.getStreamGraph.dumpStreamingPlanAsJSON()
  env.execute()
}
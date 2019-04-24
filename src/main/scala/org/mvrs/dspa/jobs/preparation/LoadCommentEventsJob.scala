package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.functions.SimpleTextFileSinkFunction
import org.mvrs.dspa.streams
import org.mvrs.dspa.utils.FlinkStreamingJob

object LoadCommentEventsJob extends FlinkStreamingJob {
  val kafkaTopic = "mvrs_comments"
  val comments = streams.comments()

  comments.map(c => s"${c.commentId};-1;${c.postId};${c.creationDate}")
    .addSink(new SimpleTextFileSinkFunction("c:\\temp\\dspa_rooted"))

  // rootedComments.addSink(utils.createKafkaProducer(kafkaTopic, Settings.config.getString("kafka.brokers"), createTypeInformation[CommentEvent]))

  println(env.getExecutionPlan) // NOTE this is the same json as env.getStreamGraph.dumpStreamingPlanAsJSON()
  env.execute()
}
package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object LoadCommentEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    streams.comments().addSink(KafkaTopics.comments.producer())

    env.execute("Import Comment events from csv file to Kafka")
  }
}


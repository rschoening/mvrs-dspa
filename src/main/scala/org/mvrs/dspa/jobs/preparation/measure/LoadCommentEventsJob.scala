package org.mvrs.dspa.jobs.preparation.measure

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object LoadCommentEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    KafkaTopics.comments.create(3, 1, overwrite = true)

    streams.comments().addSink(KafkaTopics.comments.producer())

    env.execute("Import Comment events from csv file to Kafka")
  }
}

package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object LoadPostEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    streams.posts().addSink(KafkaTopics.posts.producer())

    // execute program
    env.execute("Import post events from csv file to Kafka")
  }
}

package org.mvrs.dspa.jobs.preparation.measure

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object WritePostEventsToKafkaJob extends FlinkStreamingJob {
  def execute(): Unit = {
    KafkaTopics.posts.create(3, 1, overwrite = true)

    streams.posts().addSink(KafkaTopics.posts.producer())

    // execute program
    env.execute("Import post events from csv file to Kafka")
  }
}

package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object LoadAllEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    KafkaTopics.comments.create(3, 1, overwrite = true)
    KafkaTopics.posts.create(3, 1, overwrite = true)
    KafkaTopics.likes.create(3, 1, overwrite = true)

    streams.comments().addSink(KafkaTopics.comments.producer())
    streams.posts().addSink(KafkaTopics.posts.producer())
    streams.likes().addSink(KafkaTopics.likes.producer())

    env.execute("Import all events from csv file to Kafka")
  }
}

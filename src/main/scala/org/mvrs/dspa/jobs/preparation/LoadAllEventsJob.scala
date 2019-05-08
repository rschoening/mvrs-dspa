package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.FlinkUtils

object LoadAllEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    val speedup = Some(0.0) // don't use speedup for writing events to kafka

    FlinkUtils.writeToNewKafkaTopic(streams.comments(speedupFactorOverride = speedup), KafkaTopics.comments)

    FlinkUtils.writeToNewKafkaTopic(streams.likes(speedupFactorOverride = speedup), KafkaTopics.likes)

    FlinkUtils.writeToNewKafkaTopic(streams.posts(speedupFactorOverride = speedup), KafkaTopics.posts)

    env.execute("Import all events from csv file to Kafka")
  }
}

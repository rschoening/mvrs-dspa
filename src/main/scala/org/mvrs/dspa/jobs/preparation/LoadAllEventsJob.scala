package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.FlinkUtils

object LoadAllEventsJob extends FlinkStreamingJob(
  parallelism = 1, // important to ensure defined order (controlled by randomDelay) in Kafka
  // (otherwise order is dependent on task scheduling, with out-of-orderness orders of magnitude greater
  // than what would normally be specified for randomDelay)
  checkpointIntervalOverride = Some(0), // disable checkpointing for the load job - not fault-tolerant due to reordering step
  autoWatermarkInterval = 50
) {
  def execute(): Unit = {
    val speedup = Some(0.0) // don't use speedup for writing events to kafka

    FlinkUtils.writeToNewKafkaTopic(streams.comments(speedupFactorOverride = speedup), KafkaTopics.comments)

    FlinkUtils.writeToNewKafkaTopic(streams.likes(speedupFactorOverride = speedup), KafkaTopics.likes)

    FlinkUtils.writeToNewKafkaTopic(streams.posts(speedupFactorOverride = speedup), KafkaTopics.posts)

    env.execute("Import all events from csv file to Kafka")
  }
}

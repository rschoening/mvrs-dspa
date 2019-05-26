package org.mvrs.dspa.jobs.preparation

import org.apache.flink.api.common.JobExecutionResult
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

/**
  * Streaming job for reading events from csv files and writing them to Kafka (data preparation)
  */
object WriteEventsToKafkaJob extends FlinkStreamingJob(
  parallelism = Some(1), // important to ensure defined order (controlled by randomDelay) in Kafka
  // (otherwise order is dependent on task/buffer scheduling, with out-of-orderness orders of magnitude greater
  // than what would normally be specified for randomDelay)
  checkpointIntervalOverride = Some(0), // disable checkpointing for the load job - not fault-tolerant due to reordering step (priority queue is not checkpointed)
) {
  def execute(): JobExecutionResult = {
    val noSpeedup = Some(0.0) // don't use speedup for writing events to kafka

    FlinkUtils.writeToNewKafkaTopic(
      streams.rawComments(speedupFactorOverride = noSpeedup),
      KafkaTopics.comments,
      Settings.config.getInt("data.kafka-partition-count"),
      None,
      Settings.config.getInt("data.kafka-replica-count").toShort
    )

    FlinkUtils.writeToNewKafkaTopic(
      streams.likes(speedupFactorOverride = noSpeedup),
      KafkaTopics.likes,
      Settings.config.getInt("data.kafka-partition-count"),
      None,
      Settings.config.getInt("data.kafka-replica-count").toShort
    )

    FlinkUtils.writeToNewKafkaTopic(
      streams.posts(speedupFactorOverride = noSpeedup),
      KafkaTopics.posts,
      Settings.config.getInt("data.kafka-partition-count"),
      None,
      Settings.config.getInt("data.kafka-replica-count").toShort
    )

    FlinkUtils.printExecutionPlan()

    env.execute("Import all events from csv file to Kafka")
  }
}

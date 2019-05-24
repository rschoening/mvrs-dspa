package org.mvrs.dspa.jobs.preparation.measure

import org.apache.flink.api.common.JobExecutionResult
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics

object WriteLikeEventsToKafkaJob extends FlinkStreamingJob {
  def execute(): JobExecutionResult = {

    env.setParallelism(1)

    KafkaTopics.likes.create(1, 1, overwrite = true)

    streams
      .likes(speedupFactorOverride = Some(0))
      .addSink(KafkaTopics.likes.producer())

    // execute program
    env.execute("Import like events from csv file to Kafka")
  }
}

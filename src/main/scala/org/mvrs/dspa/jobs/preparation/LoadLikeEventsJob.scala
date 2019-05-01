package org.mvrs.dspa.jobs.preparation

import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics


object LoadLikeEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    streams.likes().addSink(KafkaTopics.likes.producer())

    // execute program
    env.execute("Import Like events from csv file to Kafka")
  }
}

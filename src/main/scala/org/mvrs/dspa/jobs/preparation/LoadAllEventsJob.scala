package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala.DataStream
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.streams
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.kafka.KafkaTopic

object LoadAllEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    val speedup = Some(0.0) // don't use speedup for writing events to kafka

    writeToNewTopic(streams.comments(speedupFactorOverride = speedup), KafkaTopics.comments)

    writeToNewTopic(streams.likes(speedupFactorOverride = speedup), KafkaTopics.likes)

    writeToNewTopic(streams.posts(speedupFactorOverride = speedup), KafkaTopics.posts)

    env.execute("Import all events from csv file to Kafka")
  }

  private def writeToNewTopic[E](stream: DataStream[E], topic: KafkaTopic[E]) = {
    topic.create(3, 1, overwrite = true)
    stream.addSink(topic.producer()).name(s"kafka: ${topic.name}")
  }
}

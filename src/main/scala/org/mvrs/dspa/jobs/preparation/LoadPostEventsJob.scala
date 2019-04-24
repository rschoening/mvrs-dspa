package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams, utils}

object LoadPostEventsJob extends FlinkStreamingJob {
  val kafkaTopic = "posts"
  val stream = streams.posts()

  stream.addSink(utils.createKafkaProducer(kafkaTopic, Settings.config.getString("kafka.brokers"), createTypeInformation[PostEvent]))

  // execute program
  env.execute("Import post events from csv file to Kafka")
}

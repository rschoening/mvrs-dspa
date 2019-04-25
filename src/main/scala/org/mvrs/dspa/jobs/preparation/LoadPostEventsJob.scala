package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.{FlinkStreamingJob, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

object LoadPostEventsJob extends FlinkStreamingJob {
  val kafkaTopic = "mvrs_posts"
  val stream = streams.posts()

  stream.addSink(FlinkUtils.createKafkaProducer(kafkaTopic, Settings.config.getString("kafka.brokers"), createTypeInformation[PostEvent]))

  // execute program
  env.execute("Import post events from csv file to Kafka")
}

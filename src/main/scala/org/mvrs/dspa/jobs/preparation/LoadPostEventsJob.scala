package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

object LoadPostEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    val kafkaTopic = "mvrs_posts"
    val stream = streams.posts()

    stream.addSink(FlinkUtils.createKafkaProducer[PostEvent](kafkaTopic, Settings.config.getString("kafka.brokers")))

    // execute program
    env.execute("Import post events from csv file to Kafka")
  }
}

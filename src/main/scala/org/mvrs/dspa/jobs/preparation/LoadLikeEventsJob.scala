package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.LikeEvent
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}


object LoadLikeEventsJob extends FlinkStreamingJob {
  val kafkaTopic = "mvrs_likes"
  val stream = streams.likes()

  stream.addSink(FlinkUtils.createKafkaProducer(kafkaTopic, Settings.config.getString("kafka.brokers"), createTypeInformation[LikeEvent]))

  // execute program
  env.execute("Import Like events from csv file to Kafka")
}

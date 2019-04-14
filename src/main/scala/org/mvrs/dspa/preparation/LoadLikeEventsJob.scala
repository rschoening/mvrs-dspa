package org.mvrs.dspa.preparation

import kantan.csv.RowDecoder
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.LikeEvent
import org.mvrs.dspa.functions.ReplayedCsvFileSourceFunction
import org.mvrs.dspa.utils


object LoadLikeEventsJob extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "likes"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  implicit val decoder: RowDecoder[LikeEvent] = LikeEvent.decoder
  val speedupFactor = 0

  val stream =
    env.addSource(
      new ReplayedCsvFileSourceFunction[LikeEvent](
        filePath,
        skipFirstLine = true, '|',
        extractEventTime = _.timestamp,
        speedupFactor = speedupFactor, // 0 -> unchanged read speed
        maximumDelayMilliseconds = 10000,
        watermarkInterval = 10000))

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[LikeEvent]))

  // execute program
  env.execute("Import Like events from csv file to Kafka")
}

package org.mvrs.dspa.jobs.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.LikeEvent
import org.mvrs.dspa.{streams, utils}


object LoadLikeEventsJob extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "likes"
  val kafkaBrokers = "localhost:9092"
  val speedupFactor = 0 // 10000
  val maximumDelayMilliseconds = 10000
  val watermarkInterval = 10000

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val stream = streams.likesFromCsv(filePath, speedupFactor, maximumDelayMilliseconds, watermarkInterval)

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[LikeEvent]))

  // execute program
  env.execute("Import Like events from csv file to Kafka")
}

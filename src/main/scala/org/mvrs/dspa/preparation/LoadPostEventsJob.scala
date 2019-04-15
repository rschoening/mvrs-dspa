package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.{streams, utils}

object LoadPostEventsJob extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "posts"
  val kafkaBrokers = "localhost:9092"
  val speedupFactor = 0 // 10000
  val maximumDelayMilliseconds = 10000
  val watermarkInterval = 10000

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val stream = streams.postsFromCsv(filePath, speedupFactor, maximumDelayMilliseconds, watermarkInterval)

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[PostEvent]))

  // execute program
  env.execute("Import post events from csv file to Kafka")
}

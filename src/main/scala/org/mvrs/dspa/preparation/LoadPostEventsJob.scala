package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostEvent
import org.mvrs.dspa.utils


object LoadPostEventsJob extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "posts"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1) // do data preparation in one worker

  val stream = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line?
    .map(PostEvent.parse _)

  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[PostEvent]))

  // execute program
  env.execute("Import post events from csv file to Kafka")
}

package org.mvrs.dspa.preparation

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.LikeEvent
import org.mvrs.dspa.utils


object LoadLikeEvents extends App {
  require(args.length == 1, "full path to csv file expected")

  val filePath: String = args(0)
  val kafkaTopic = "likes"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val stream = env
    .readTextFile(filePath)
    .filter(!_.startsWith("Person.id|")) // TODO better way to skip the header line?
    .map(LikeEvent.parse _)
  //    .keyBy(_.postId) // not currently needed

  // stream.print
  stream.addSink(utils.createKafkaProducer(kafkaTopic, kafkaBrokers, createTypeInformation[LikeEvent]))

  // execute program
  env.execute("Import Like events from csv file to Kafka")
}

package org.mvrs.dspa.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.mvrs.dspa.utils

class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime, parallelism: Int = 4) extends App {
  private val localWithUI = args.length > 0 && args(0) == "local-with-ui"

  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(localWithUI)
  env.setStreamTimeCharacteristic(timeCharacteristic)
  env.setParallelism(4)
}

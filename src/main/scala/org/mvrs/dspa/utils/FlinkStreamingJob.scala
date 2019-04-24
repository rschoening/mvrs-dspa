package org.mvrs.dspa.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.mvrs.dspa.utils


abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Int = 4) extends FlinkJob {
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(localWithUI)

  env.setStreamTimeCharacteristic(timeCharacteristic)
  env.setParallelism(4)
}



package org.mvrs.dspa.utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Int = 4) extends FlinkJob {
  implicit val env: StreamExecutionEnvironment = FlinkUtils.createStreamExecutionEnvironment(localWithUI)

  env.setStreamTimeCharacteristic(timeCharacteristic)
  env.setParallelism(4)
}



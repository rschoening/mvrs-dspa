package org.mvrs.dspa.jobs

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.mvrs.dspa.utils.FlinkUtils


abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Int = 4) extends FlinkJob {
  implicit val env: StreamExecutionEnvironment = FlinkUtils.createStreamExecutionEnvironment(localWithUI)

  env.setStreamTimeCharacteristic(timeCharacteristic)
  env.setParallelism(4)
}



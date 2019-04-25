package org.mvrs.dspa.utils

import org.apache.flink.api.scala.ExecutionEnvironment

abstract class FlinkBatchJob(parallelism: Int = 4) extends FlinkJob {
  implicit val env: ExecutionEnvironment = FlinkUtils.createBatchExecutionEnvironment(localWithUI)

  env.setParallelism(4)
}

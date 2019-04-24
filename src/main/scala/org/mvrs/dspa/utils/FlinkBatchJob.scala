package org.mvrs.dspa.utils

import org.apache.flink.api.scala.ExecutionEnvironment
import org.mvrs.dspa.utils

abstract class FlinkBatchJob(parallelism: Int = 4) extends FlinkJob {
  implicit val env: ExecutionEnvironment = utils.createBatchExecutionEnvironment(localWithUI)

  env.setParallelism(4)
}

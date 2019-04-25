package org.mvrs.dspa.jobs

import org.apache.flink.api.scala.ExecutionEnvironment
import org.mvrs.dspa.utils.FlinkUtils

abstract class FlinkBatchJob(parallelism: Int = 4) extends FlinkJob {
  implicit val env: ExecutionEnvironment = FlinkUtils.createBatchExecutionEnvironment(localWithUI)

  env.setParallelism(4)
}

package org.mvrs.dspa.jobs

import org.apache.flink.api.scala.ExecutionEnvironment
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Utility base class for flink batch jobs
  *
  * @param parallelism the default parallelism for the job
  */
abstract class FlinkBatchJob(parallelism: Int = 4) extends FlinkJob {
  implicit val env: ExecutionEnvironment = FlinkUtils.createBatchExecutionEnvironment(localWithUI)

  env.setParallelism(parallelism)

  execute()
}

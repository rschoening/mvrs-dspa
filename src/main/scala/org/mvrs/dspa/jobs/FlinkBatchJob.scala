package org.mvrs.dspa.jobs

import org.apache.flink.api.scala.ExecutionEnvironment
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Utility base class for flink batch jobs
  *
  * @param parallelism the default parallelism for the job
  */
abstract class FlinkBatchJob(parallelism: Option[Int] = None) extends FlinkJob {
  // localWithUI is set by base class based on program arguments

  ExecutionEnvironment.setDefaultLocalParallelism(defaultLocalParallelism)

  // NOTE: environment is set by base class based on program arguments
  implicit val env: ExecutionEnvironment = environmentType match {
    case EnvironmentType.Default => FlinkUtils.createBatchExecutionEnvironment()
    case EnvironmentType.LocalWithUI => FlinkUtils.createBatchExecutionEnvironment(true)
    case EnvironmentType.Remote => FlinkUtils.createRemoteBatchExecutionEnvironment(flinkClusterHost, flinkClusterPort, flinkClusterJars: _*)
  }

  // apply override of default parallelism, if defined
  parallelism.foreach(env.setParallelism)

  executeJob()
}

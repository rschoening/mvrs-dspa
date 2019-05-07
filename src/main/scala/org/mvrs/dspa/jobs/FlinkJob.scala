package org.mvrs.dspa.jobs

/**
  * Utility base class for flink jobs.
  * Initializes state based on job arguments
  */
abstract class FlinkJob() extends App {
  protected val localWithUI = args.length > 0 && args(0) == "local-with-ui" // inidicates that job should use local cluster *with* UI and metrics

  def execute(): Unit
}

package org.mvrs.dspa.jobs

abstract class FlinkJob() extends App {
  protected val localWithUI = args.length > 0 && args(0) == "local-with-ui"

  def execute(): Unit
}

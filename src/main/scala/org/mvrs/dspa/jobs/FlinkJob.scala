package org.mvrs.dspa.jobs

import org.slf4j.LoggerFactory

/**
  * Utility base class for flink jobs.
  * Initializes state based on job arguments
  */
abstract class FlinkJob() extends App {
  protected val localWithUI = args.length > 0 && args(0) == "local-with-ui" // inidicates that job should use local cluster *with* UI and metrics
  private lazy val LOG = LoggerFactory.getLogger(getClass)

  def execute(): Unit

  protected def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)

  protected def info(msg: => String): Unit = if (LOG.isInfoEnabled()) LOG.info(msg)

  protected def warn(msg: => String): Unit = if (LOG.isWarnEnabled()) LOG.warn(msg)
}

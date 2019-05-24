package org.mvrs.dspa.jobs

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.JobExecutionResult
import org.mvrs.dspa.Settings
import org.mvrs.dspa.jobs.EnvironmentType.EnvironmentType
import org.mvrs.dspa.utils.DateTimeUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Utility base class for flink jobs.
  * Initializes state based on job arguments
  */
abstract class FlinkJob() extends App {
  private lazy val LOG = LoggerFactory.getLogger(getClass)

  // read settings common to batch and streaming jobs
  protected val defaultLocalParallelism = Settings.config.getInt("jobs.default-local-parallelism")
  protected val flinkClusterHost = Settings.config.getString("jobs.remote-env.host")
  protected val flinkClusterPort = Settings.config.getInt("jobs.remote-env.port")
  protected val flinkClusterJars = Settings.config.getStringList("jobs.remote-env.jars").asScala

  protected val environmentType = getEnvironmentFromArgs(args)

  protected def executeJob(): Unit = {
    val result = execute()

    println(s"Job finished in ${DateTimeUtils.formatDuration(result.getNetRuntime(TimeUnit.MILLISECONDS))}")
  }

  /**
    * Template method for subclasses to set up and submit the job graph
    *
    * @return
    */
  def execute(): JobExecutionResult

  protected def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)

  protected def info(msg: => String): Unit = if (LOG.isInfoEnabled()) LOG.info(msg)

  protected def warn(msg: => String): Unit = if (LOG.isWarnEnabled()) LOG.warn(msg)

  /**
    * Get the environment type from program arguments
    *
    * @param args
    * @return
    */
  private def getEnvironmentFromArgs(args: Array[String]): EnvironmentType =
    if (args.length == 0) EnvironmentType.Default
    else if (args.length == 1) args(0) match {
      case "local-with-ui" => EnvironmentType.LocalWithUI
      case "remote" => EnvironmentType.Remote
    }
    else throw new IllegalArgumentException(args.toList.toString)
}

object EnvironmentType extends Enumeration {
  type EnvironmentType = Value
  val Default, LocalWithUI, Remote = Value
}

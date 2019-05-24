package org.mvrs.dspa.jobs

import org.mvrs.dspa.Settings
import org.mvrs.dspa.jobs.EnvironmentType.Environment
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

  def execute(): Unit

  protected def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)

  protected def info(msg: => String): Unit = if (LOG.isInfoEnabled()) LOG.info(msg)

  protected def warn(msg: => String): Unit = if (LOG.isWarnEnabled()) LOG.warn(msg)

  private def getEnvironmentFromArgs(args: Array[String]): Environment =
    if (args.length == 0) EnvironmentType.Default
    else if (args.length == 1) args(0) match {
      case "local-with-ui" => EnvironmentType.LocalWithUI
      case "remote" => EnvironmentType.Remote
    }
    else throw new IllegalArgumentException(args.toList.toString)
}

object EnvironmentType extends Enumeration {
  type Environment = Value
  val Default, LocalWithUI, Remote = Value
}

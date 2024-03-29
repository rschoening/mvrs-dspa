package org.mvrs.dspa.jobs

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.TernaryBoolean
import org.mvrs.dspa.Settings
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Utility base class to simplify implementation of flink jobs. Applies settings from config file.
  *
  * @param timeCharacteristic the time characteristic for the job
  * @param parallelism        optional override of the default parallelism for the job
  * @param enableGenericTypes indicates if generic types should be enabled.
  *                           NOTE due to https://issues.apache.org/jira/browse/FLINK-12410, this has to be enabled
  *                           when reading from Kafka topics. Same also for text input format (generic serialization is
  *                           used for input splits).
  */
abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Option[Int] = None,
                                 enableGenericTypes: Boolean = false,
                                 checkpointIntervalOverride: Option[Long] = None) extends FlinkJob {

  // read settings

  // NOTE
  // - read job-independent global preferences from settings (application.conf)
  // - get job-dependent settings via constructor arguments
  // To make this class more general-purpose, read settings in project-specific subclass and provide constructor args for all these here
  private val autoWatermarkInterval = Settings.duration("jobs.auto-watermark-interval").toMilliseconds
  private val latencyTrackingInterval = Settings.config.getInt("jobs.latency-tracking-interval")
  private val stateBackendRocksDb = Settings.config.getBoolean("jobs.state-backend-rocksdb")
  private val stateBackendPath = Settings.config.getString("jobs.state-backend-path")
  private val rocksDbPath = Settings.config.getString("jobs.rocksdb-path")
  private val checkpointInterval = checkpointIntervalOverride.getOrElse(Settings.duration("jobs.checkpoint-interval").toMilliseconds)
  private val checkpointMinPause = Settings.duration("jobs.checkpoint-min-pause").toMilliseconds
  private val checkpointIncrementally = Settings.config.getBoolean("jobs.checkpoint-incrementally")
  private val asynchronousSnapshots = Settings.config.getBoolean("jobs.asynchronous-snapshots")
  private val restartAttempts = Settings.config.getInt("jobs.restart-attempts")
  private val delayBetweenAttempts = Settings.config.getLong("jobs.delay-between-attempts")

  StreamExecutionEnvironment.setDefaultLocalParallelism(defaultLocalParallelism)

  // NOTE: environment is set by base class based on program arguments
  implicit val env: StreamExecutionEnvironment = environmentType match {
    case EnvironmentType.Default => FlinkUtils.createStreamExecutionEnvironment()
    case EnvironmentType.LocalWithUI => FlinkUtils.createStreamExecutionEnvironment(true)
    case EnvironmentType.Remote => FlinkUtils.createRemoteStreamExecutionEnvironment(flinkClusterHost, flinkClusterPort, flinkClusterJars: _*)
  }

  // see https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#latency-tracking
  if (latencyTrackingInterval > 0) {
    env.getConfig.setLatencyTrackingInterval(latencyTrackingInterval)
  }

  if (!enableGenericTypes) {
    // NOTE:
    // - this fails when using streams of tuples in scala, unless type information is provided using createTypeInformation[T] (instead of classOf[T])
    // - also fails when reading from kafka partitions (https://issues.apache.org/jira/browse/FLINK-12410)
    // - and also when reading from text input format (splits)
    env.getConfig.disableGenericTypes() // to make sure the warning can't be overlooked
  }

  if (checkpointInterval > 0) {
    env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpointMinPause)
  }

  if (restartAttempts > 0) {
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))
  }

  if (stateBackendPath.length > 0) {

    val stateBackend: StateBackend = new FsStateBackend(stateBackendPath, asynchronousSnapshots)

    if (stateBackendRocksDb) {
      val rocksDbStateBackend = new RocksDBStateBackend(stateBackend, TernaryBoolean.fromBoolean(checkpointIncrementally))
      rocksDbStateBackend.setDbStoragePath(rocksDbPath)

      env.setStateBackend(rocksDbStateBackend.asInstanceOf[StateBackend])
    }
    else {
      env.setStateBackend(stateBackend)
    }
  }

  env.getConfig.setAutoWatermarkInterval(autoWatermarkInterval)
  env.setStreamTimeCharacteristic(timeCharacteristic)

  // apply override of default parallelism, if defined
  parallelism.foreach(env.setParallelism)

  executeJob()
}
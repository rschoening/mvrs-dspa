package org.mvrs.dspa.jobs

import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Utility base class to simplify implementation of flink jobs. Applies settings from config file.
  *
  * @param timeCharacteristic the time characteristic for the job
  * @param parallelism        the default parallelism for the job
  * @param enableGenericTypes indicates if generic types should be enabled.
  *                           NOTE due to https://issues.apache.org/jira/browse/FLINK-12410, this has to be enabled
  *                           when reading from Kafka topics. Same also for text input format (generic serialization is
  *                           used for input splits).
  */
abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Int = 4,
                                 enableGenericTypes: Boolean = false) extends FlinkJob {
  // read settings
  // - to make the class more general-purpose, read settings in project-specific subclass and
  //   provide ctor args for all these here
  private val latencyTrackingInterval = Settings.config.getInt("jobs.latency-tracking-interval")
  private val stateBackendPath = Settings.config.getString("jobs.state-backend-path")
  private val checkpointInterval = Settings.config.getLong("jobs.checkpoint-interval")

  implicit val env: StreamExecutionEnvironment = FlinkUtils.createStreamExecutionEnvironment(localWithUI)

  // see https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/metrics.html#latency-tracking
  env.getConfig.setLatencyTrackingInterval(latencyTrackingInterval)

  if (!enableGenericTypes) {
    // NOTE:
    // - this fails when using streams of tuples in scala, unless type information is provided using createTypeInformation[T] (instead of classOf[T])
    // - also fails when reading from kafka partitions (https://issues.apache.org/jira/browse/FLINK-12410)
    // - and also when reading from text input format (splits)
    env.getConfig.disableGenericTypes() // to make sure the warning can't be overlooked
  }

  if (checkpointInterval > 0)
    env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE)

  if (stateBackendPath.length > 0) {

    // NOTE rocksdb checkpoints fail due to long temporary directory path (>256 chars)
    // at org.rocksdb.Checkpoint.createCheckpoint(Native Method)
    // val stateBackend: StateBackend = new RocksDBStateBackend(stateBackendPath, true)

    val stateBackend: StateBackend = new FsStateBackend(stateBackendPath, false)
    env.setStateBackend(stateBackend)
  }

  env.setStreamTimeCharacteristic(timeCharacteristic)
  env.setParallelism(parallelism)

  execute() // call template method where subclass sets up job
}



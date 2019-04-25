package org.mvrs.dspa.jobs

import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.utils.FlinkUtils


abstract class FlinkStreamingJob(timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
                                 parallelism: Int = 4) extends FlinkJob {
  // read settings
  private val latencyTrackingInterval = Settings.config.getInt("jobs.latency-tracking-interval")
  private val stateBackendPath = Settings.config.getString("jobs.state-backend-path")
  private val checkpointInterval = Settings.config.getLong("jobs.checkpoint-interval")

  implicit val env: StreamExecutionEnvironment = FlinkUtils.createStreamExecutionEnvironment(localWithUI)

  env.getConfig.setLatencyTrackingInterval(latencyTrackingInterval)

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
  env.setParallelism(4)
}



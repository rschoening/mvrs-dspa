package org.mvrs.dspa.jobs.clustering.measure

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.{ProgressInfo, ProgressMonitorFunction}
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.LikeEvent
import org.mvrs.dspa.streams
import org.mvrs.dspa.utils.DateTimeUtils

object ReadLikesFromKafkaJob extends FlinkStreamingJob(enableGenericTypes = true) {

  def execute(): Unit = {
    env.setParallelism(4)

    val now = System.currentTimeMillis()
    streams
      .likesFromKafka("testConsumer", 0, Time.hours(0))
      .process(new ProgressMonitorFunction[LikeEvent]())
      //.filter(p => p.isLate) //  && p.totalCountSoFar % 100 == 0)
      //.filter(p => p.isLate || p.isBehindNewest)
      .map(p => format(p))
      .print

    // Observations
    // 1) single partition, 1 writer task
    //    - late events:        NONE
    //    - unordered events:   NONE

    // 2) single partition, 4 writer tasks, max-out-of-orderness = 0
    //    - late events:       18298 late events, max lateness: 57 days
    //    - unordered events: 488640 events, max behindness: 235 days (diff. to current max. timestamp seen by worker)

    // 3) three partitions, 1 writer task
    //    - late events:        NONE
    //    - unordered events:   NONE

    // NOTE: number of READER tasks does not play a role
    // NOTE: if there is just one partition, that partition will be read only by ONE of the tasks. A partition is consumed fully by a single consumer

    // --> consequence:
    // - loading into Kafka should either be done under realistic insert rates (low speedup values)
    //   OR by a SINGLE worker

    env.execute("Read likes from Kafka")
  }

  def format(p: ProgressInfo[LikeEvent]): String =
    s"subtask: ${p.subtask} " +
      (if (p.isBehindNewest) s"- behind by: ${DateTimeUtils.formatDuration(p.millisBehindNewest)} " else " - ") +
      (if (p.isLate) s"- late by: ${DateTimeUtils.formatDuration(p.millisBehindWatermark)} " else " - ") +
      "|| " +
      s"- late: ${p.lateCountSoFar} " +
      s"- behind: ${p.behindNewestCountSoFar} " +
      s"- total: ${p.totalCountSoFar} " +
      s"- max. lateness: ${DateTimeUtils.formatDuration(p.maximumLatenessSoFar)} " +
      s"- max. behindness: ${DateTimeUtils.formatDuration(p.maximumBehindnessSoFar)}"

}

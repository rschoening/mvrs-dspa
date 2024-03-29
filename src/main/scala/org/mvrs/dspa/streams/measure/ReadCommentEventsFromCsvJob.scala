package org.mvrs.dspa.streams.measure

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.ProgressMonitorFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.RawCommentEvent
import org.mvrs.dspa.streams

object ReadCommentEventsFromCsvJob extends FlinkStreamingJob(
  parallelism = Some(1),
  checkpointIntervalOverride = Some(0)) {

  def execute(): JobExecutionResult = {
    env.getConfig.setAutoWatermarkInterval(1)
    streams.rawComments(speedupFactorOverride = Some(0))
      .process(new ProgressMonitorFunction[RawCommentEvent])
      .filter(t => t._2.isLate || t._2.elementCount % 100000 == 0 || t._2.watermarkIncrement > 1000 * 3600)
      .map(_._2.toString)
      .print()

    // Observations
    // ------------
    // speedup = 20000, parallelism = 1, random-delay = 30 minutes
    // - no late elements
    // - max behind: 2h 43 min

    // speedup = 20000, parallelism = 1, random-delay = 0
    // - no late elements
    // - max behind: 1h 38min up to 473000 TODO
    // TODO check max watermark interval (event time increment, max and histogram)

    // speedup = 0, parallelism = 4, random-delay = 30 minutes
    // - no late elements
    // - max. behind: 240h !!!!!

    // speedup = 0, parallelism = 4, random-delay = 0
    // - no late elements
    // - max behind: 237 h

    // interpretation:
    // ---------------
    // the auto-watermark interval (proc. time) results in a distribution of event-time watermark increments. Depending on the wm-related logic,
    // this may lead to out-of-ordering of that increment magnitude (even with maxOutofOrderness = 0 in timestamp/wm assigner - any further maxOutOfOrderness is additive).

    env.execute("Read Comment events from csv file")
  }
}

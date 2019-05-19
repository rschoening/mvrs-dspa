package org.mvrs.dspa.streams.measure

import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.ProgressMonitorFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.CommentEvent
import org.mvrs.dspa.streams

object ReadCommentEventsFromCsv extends FlinkStreamingJob(parallelism = 1) {
  def execute(): Unit = {
    streams.comments()
      .process(new ProgressMonitorFunction[CommentEvent])
      .filter(t => t._2.isLate || t._2.totalCountSoFar % 100 == 0)
      .map(_._2.toString)
      .print()

    // Observations
    // ------------
    // speedup = 20000, parallelism = 1, random-delay = 30 minutes
    // - no late elements
    // - max behind: 80h !!!!!
    // (up to element 35000)

    // speedup = 0, parallelism = 4, random-delay = 30 minutes
    // - no late elements
    // - max. behind: 240h !!!!!

    // speedup = 0, parallelism = 4, random-delay = 0
    // - no late elements
    // - max behind: 237 h

    // speedup = 20000, parallelism = 1, random-delay = 0
    // - no late elements
    // - max behind: 78h

    env.execute("Read Comment events from csv file")
  }
}

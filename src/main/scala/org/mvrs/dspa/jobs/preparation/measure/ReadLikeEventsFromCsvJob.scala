package org.mvrs.dspa.jobs.preparation.measure

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.SimpleTextFileSinkFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams}

object ReadLikeEventsFromCsvJob extends FlinkStreamingJob {
  def execute(): JobExecutionResult = {
    streams
      .likesFromCsv(Settings.config.getString("data.likes-csv-path"), speedupFactor = 100000)
      .map(_.toString)
      .addSink(new SimpleTextFileSinkFunction("c:\\temp\\likes")) // NOTE any temp file

    // execute program
    env.execute("Write like events to text files for measurements")

    // event count in 1k file: 662890
    // start date: 2012-02-02T01:09:00.000Z
    // end date:   2013-02-19T06:32:17.000Z
    // -> ~ 550000 minutes

    // no speedup definition: 7 seconds
    // - emitted watermarks: 3311060
    // - after change to minimum interval (1000ms): 5 (no change to processing time)

    // with speedup factor 100000, wm interval 10000 ms
    // - duration: 5 min 39 seconds (expected: ~5.5 minutes + overhead) --> OK
    // - ~10000 watermarks per second scheduled!
    // --> need to apply absolute minimum interval (how to do this in case of unscaled replay? can't schedule based on event time...)
    //     - after change to minimum interval (1000ms): 332 (NOTE this will no longer be deterministic)

    // with speedup factor 200000, wm interval 10000 ms
    // - duration: 2 min 49 seconds (expected: ~2.75 minutes + overhead) --> OK
    // - ~20000 watermarks per second scheduled! - 1 / sec after change to minimum interval (1000ms)
    // - total emitted watermarks: 3311060 (167 after change to minimum interval 1000ms)
  }
}

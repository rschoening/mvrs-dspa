package org.mvrs.dspa.jobs.preparation.measure

import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.SimpleTextFileSinkFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams}

object LoadLikeEventsJob extends FlinkStreamingJob {
  def execute(): Unit = {
    streams
      .likesFromCsv(Settings.config.getString("data.likes-csv-path"))
      .map(_.toString)
      .addSink(new SimpleTextFileSinkFunction("c:\\temp\\likes"))

    // execute program
    env.execute("Write like events to text files for measurements")
  }
}

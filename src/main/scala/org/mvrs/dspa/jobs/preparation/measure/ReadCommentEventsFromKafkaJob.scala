package org.mvrs.dspa.jobs.preparation.measure

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.ProgressMonitorFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.CommentEvent
import org.mvrs.dspa.streams

object ReadCommentEventsFromKafkaJob extends FlinkStreamingJob(enableGenericTypes = true) {

  def execute(): JobExecutionResult = {
    env.setParallelism(4)

    env.getConfig.setAutoWatermarkInterval(1L)

    streams
      .commentsFromKafka(
        "testConsumer",
        0,
        Time.minutes(0),
        lookupParentPostId = replies => replies.map(Left(_)))._1
      .startNewChain()
      .process(new ProgressMonitorFunction[CommentEvent]())
      .map(_._2)
      .filter(p => p.isLate || p.elementCount % 25000 == 0)
      .map(_.toString)
      .print

    // event count in 1k file: 632043
    // start date: 2012-02-02T02:45:14Z
    // end date:   2013-02-17T11:50:23Z
    // -> ~ 550000 minutes

    // no speedup definition, 4 workers:
    // - duration: 15 seconds (corresponds to a speedup factor of ~2'200'000)
    // - 11 K events per second per worker (1 broker, 4 workers, one partition)

    // no speedup definition, 1 worker:
    // - duration: 14 seconds (corresponds to a speedup factor of ~2'300'000)
    // - 40 K events per second (1 broker, one worker, one partition)

    // with speedup factor 100000, 4 workers
    // - duration: 5 min 19 seconds (expected: ~5.5 minutes) --> OK

    // with speedup factor 200000, 4 workers
    // - duration: 2 min 40 seconds (expected: ~2.75 minutes) --> OK

    env.execute("Read comments from Kafka")
  }
}

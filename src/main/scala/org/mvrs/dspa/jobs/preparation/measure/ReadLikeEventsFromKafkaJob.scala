package org.mvrs.dspa.jobs.preparation.measure

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.mvrs.dspa.functions.ProgressMonitorFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.LikeEvent
import org.mvrs.dspa.streams

object ReadLikeEventsFromKafkaJob extends FlinkStreamingJob(enableGenericTypes = true) {

  def execute(): Unit = {
    env.setParallelism(4)

    env.getConfig.setAutoWatermarkInterval(1L)

    streams
      .likesFromKafka("testConsumer", 0, Time.minutes(30)).startNewChain()
      .process(new ProgressMonitorFunction[LikeEvent]())
      .map(_._2)
//      .filter(p => p.totalCountSoFar % 10000 == 0)
      // .filter(_.subtask == 1)
      //.filter(p => p.watermarkAdvanced || p.isLate || p.isBehindNewest)
      .filter(p => p.isLate || p.elementCount % 100000 == 0) //  || p.isBehindNewest || p.totalCountSoFar % 10000 == 0)
      .map(_.toString)
      .print

    // PROBLEM 1: unordered / late events
    // ----------------------------------
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

    // PROBLEM 2: watermark emission
    // -----------------------------
    // first watermark is generated at 2012-02-13 (13 event time days after start) for interval = 10 ms
    // - after 2012-02-05 for interval = 1!
    // --> set interval low

    // autowatermark interval -> first watermark (speedup = 20000)
    // 1ms   -> 2012-02-07 / 08
    // 10ms  -> 2012-02-08
    // 100ms -> 2012-02-13

    // INCORRECT: NOTE when specifying a speedupfactor > 0 then there are NO watermarks issued (when extracting watermarks PER PARTITION) when doing it AFTERWARDS
    // - after the SimpleScaledReplay map function: watermarks are issued normally


    env.execute("Read likes from Kafka")
  }
}

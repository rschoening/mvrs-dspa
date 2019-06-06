package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams}

/**
  * Streaming job for reading post statistics from Kafka, enriching them based on post information available in
  * ElasticSearch, and writing them to ElasticSearch (DSPA Task #1)
  */
object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): JobExecutionResult = {
    // read settings
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    val batchSize = Settings.config.getInt("jobs.active-post-statistics.post-statistics-elasticsearch-batch-size")

    val esIndex = ElasticSearchIndexes.activePostStatistics
    esIndex.create()

    val postStatisticsStream: DataStream[PostStatistics] = streams.postStatistics("move-post-statistics")

    val enrichedStream: DataStream[(PostStatistics, String, String)] = enrichPostStatistics(postStatisticsStream)

    enrichedStream.addSink(esIndex.createSink(batchSize))
      .name(s"ElasticSearch: ${esIndex.indexName}")

    FlinkUtils.printExecutionPlan()
    FlinkUtils.printOperatorNames()

    // print complete progress information for the statistics for a popular post (943850)
    // - if data.random-delay (in application.conf) is < than the slide of the post statistics window, there should
    //   never be any "behind" events, i.e. statistics for a given post must be strictly ordered by timestamp:
    // -> in the progress monitor output:
    //   - bn (time behind newest) must always be 'latest'
    //   - bnc (behind events seen so far) must be 0
    //   - bmx (maximum time behind newest seen so far) must be -
    FlinkUtils.addProgressMonitor(enrichedStream.filter(_._1.postId == 943850), prefix = "POST")()

    // Uncomment the line below to print progress information for any late events
    //
    //   Note that there will be late events, since we're reading from multiple partitions and due to the behavior of
    //   the watermark emission (stopping the auto-interval clock during backpressure phases), the watermarks have to
    //   be generated after the replay function, i.e. _not_ per partition.
    //
    //   However, these late events are no problem for the result in ElasticSearch.
    //
    // FlinkUtils.addProgressMonitor(enrichedStream, prefix = "LATE") { case (_, progressInfo) => progressInfo.isLate }

    // execute program
    env.execute("Move enriched post statistics from Kafka to ElasticSearch")
  }

  private def enrichPostStatistics(postStatisticsStream: DataStream[PostStatistics])
                                  (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(PostStatistics, String, String)] =
    FlinkUtils.asyncStream(
      postStatisticsStream,
      new AsyncEnrichPostStatisticsFunction(
        ElasticSearchIndexes.postInfos.indexName,
        esNodes: _*
      )
    ).name("Async I/O: enrich post statistics")
}



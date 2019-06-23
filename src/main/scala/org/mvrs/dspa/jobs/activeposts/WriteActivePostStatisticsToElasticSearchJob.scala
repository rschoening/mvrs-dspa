package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams}

import scala.collection.JavaConverters._

/**
  * Streaming job for reading post statistics from Kafka, enriching them based on post information available in
  * ElasticSearch, and writing them to ElasticSearch (DSPA Task #1)
  */
object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): JobExecutionResult = {
    // read settings
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    val batchSize = Settings.config.getInt("jobs.active-post-statistics.to-elasticsearch.batch-size")
    val monitoredPosts = Settings.config.getLongList("jobs.active-post-statistics.to-elasticsearch.monitored-posts").asScala.toSet
    val monitorLateness = Settings.config.getBoolean("jobs.active-post-statistics.to-elasticsearch.monitor-lateness")

    // (re)create ElasticSearch index for post statistics
    val esIndex = ElasticSearchIndexes.activePostStatistics
    esIndex.create()

    // consume post statistics from Kafka
    val postStatisticsStream: DataStream[PostStatistics] = streams.postStatistics("move-post-statistics")

    // enrich post statistics with post content and forum title
    val enrichedStream: DataStream[(PostStatistics, String, String)] = enrichPostStatistics(postStatisticsStream)

    // write to enriched statistics to ElasticSearch
    enrichedStream.addSink(esIndex.createSink(batchSize))
      .name(s"ElasticSearch: ${esIndex.indexName}")

    FlinkUtils.printExecutionPlan()
    FlinkUtils.printOperatorNames()

    // monitor progress information on statistics records for selected posts configured in settings
    if (monitoredPosts.nonEmpty)
      FlinkUtils.addProgressMonitor(
        enrichedStream.filter(t => monitoredPosts.contains(t._1.postId)),
        prefix = "POST")()

    // monitor late events if configured in settings
    if (monitorLateness)
      FlinkUtils.addProgressMonitor(enrichedStream, prefix = "LATE") { case (_, progressInfo) => progressInfo.isLate }

    // execute program
    env.execute("Move enriched post statistics from Kafka to ElasticSearch")
  }

  /**
    * Enriches the stream of post statistics with post content and forum title
    *
    * @param postStatisticsStream The input stream of post statistics
    * @param esNodes              The ElasticSearch nodes to connect to
    * @return Stream of tuples (post statistics, post content or image file, forum title)
    */
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
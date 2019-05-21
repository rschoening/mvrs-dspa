package org.mvrs.dspa.jobs.activeposts

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
  def execute(): Unit = {
    // read settings
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    val batchSize = Settings.config.getInt("jobs.active-post-statistics.post-statistics-elasticsearch-batch-size")

    val esIndex = ElasticSearchIndexes.activePostStatistics
    esIndex.create()

    val postStatisticsStream: DataStream[PostStatistics] = streams.postStatistics("move-post-statistics")

    enrichPostStatistics(postStatisticsStream)
      .disableChaining() // disable chaining to be able to observe back-pressure on preceding operators NOTE revise, see observations for recommendation job
      .addSink(esIndex.createSink(batchSize))
      .name(s"ElasticSearch: ${esIndex.indexName}")
      .disableChaining()

    FlinkUtils.printExecutionPlan()

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



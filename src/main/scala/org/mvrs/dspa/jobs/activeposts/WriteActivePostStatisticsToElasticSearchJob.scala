package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams}

object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): Unit = {
    // read settings
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    val batchSize = Settings.config.getInt("jobs.active-post-statistics.post-statistics-elasticsearch-batch-size")

    val esIndex = ElasticSearchIndexes.activePostStatistics;
    esIndex.create()

    val postStatisticsStream: DataStream[PostStatistics] = streams.postStatistics("move-post-statistics")

    enrichPostStatistics(postStatisticsStream)
      .addSink(esIndex.createSink(batchSize))
      .name(s"ElasticSearch: ${esIndex.indexName}")


    // execute program
    env.execute("Move enriched post statistics from Kafka to ElasticSearch")
  }

  private def enrichPostStatistics(postStatisticsStream: DataStream[PostStatistics])
                                  (implicit elasticSearchNodes: Seq[ElasticSearchNode]): DataStream[(PostStatistics, String, String)] =
    FlinkUtils.asyncStream(
      postStatisticsStream,
      new AsyncEnrichPostStatisticsFunction(
        ElasticSearchIndexes.postInfos.indexName,
        elasticSearchNodes: _*
      )
    )
}



package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.streams.KafkaTopics
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.elastic.ElasticSearchNode


object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob {
  def execute(): Unit = {
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes

    ElasticSearchIndexes.activePostStatistics.create()

    val postStatisticsStream: DataStream[PostStatistics] =
      env.addSource(KafkaTopics.postStatistics.consumer("move-post-statistics"))

    val enrichedStream: DataStream[(PostStatistics, String, String)] = enrichPostStatistics(postStatisticsStream)

    enrichedStream.addSink(ElasticSearchIndexes.activePostStatistics.createSink(100))

    // execute program
    env.execute("Move enriched post statistics from Kafka to ElasticSearch")
  }

  private def enrichPostStatistics(postStatisticsStream: DataStream[PostStatistics])
                                  (implicit elasticSearchNodes: Seq[ElasticSearchNode]): DataStream[(PostStatistics, String, String)] = {
    FlinkUtils.asyncStream(
      postStatisticsStream,
      new AsyncEnrichPostStatisticsFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        elasticSearchNodes: _*
      )
    )
  }
}



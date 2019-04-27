package org.mvrs.dspa.jobs.activeposts

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.elastic.ElasticSearchNode
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.PostStatistics
import org.mvrs.dspa.utils.FlinkUtils


object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob {
  def execute(): Unit = {
    val kafkaBrokers = Settings.config.getString("kafka.brokers")
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes

    ElasticSearchIndexes.activePostStatistics.create()

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", "test")
    props.setProperty("isolation.level", "read_committed")

    val source = FlinkUtils.createKafkaConsumer("mvrs_poststatistics", createTypeInformation[PostStatistics], props)

    val postStatisticsStream = env.addSource(source)

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



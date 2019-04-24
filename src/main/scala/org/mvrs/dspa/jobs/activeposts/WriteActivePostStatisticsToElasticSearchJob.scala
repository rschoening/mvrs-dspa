package org.mvrs.dspa.jobs.activeposts

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, utils}


object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob {

  val kafkaBrokers = Settings.config.getString("kafka.brokers")

  ElasticSearchIndexes.activePostStatistics.create()

  val props = new Properties()
  props.setProperty("bootstrap.servers", kafkaBrokers)
  props.setProperty("group.id", "test")
  props.setProperty("isolation.level", "read_committed")

  val source = utils.createKafkaConsumer("mvrs_poststatistics", createTypeInformation[PostStatistics], props)

  val postStatisticsStream = env.addSource(source)

  val enrichedStream =
    utils.asyncStream(
      postStatisticsStream,
      new AsyncEnrichPostStatisticsFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        Settings.elasticSearchNodes: _*
      )
    )

  enrichedStream.addSink(ElasticSearchIndexes.activePostStatistics.createSink(100))

  // execute program
  env.execute("Move enriched post statistics from Kafka to ElasticSearch")
}



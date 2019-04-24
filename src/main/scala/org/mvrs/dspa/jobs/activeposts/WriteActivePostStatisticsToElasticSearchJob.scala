package org.mvrs.dspa.jobs.activeposts

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, utils}


object WriteActivePostStatisticsToElasticSearchJob extends FlinkStreamingJob {

  val indexName = "active-post-statistics"
  val kafkaBrokers = Settings.config.getString("kafka.brokers")

  val index = new ActivePostStatisticsIndex(indexName, Settings.elasticSearchNodes(): _*)
  index.create()

  val props = new Properties()
  props.setProperty("bootstrap.servers", kafkaBrokers)
  props.setProperty("group.id", "test")
  props.setProperty("isolation.level", "read_committed")

  val source = utils.createKafkaConsumer("poststatistics", createTypeInformation[PostStatistics], props)

  // TODO look up post information (content etc.) with asyncfunction
  val stream = env
    .addSource(source)
    .addSink(index.createSink(100))

  // execute program
  env.execute("Move enriched post statistics from Kafka to ElasticSearch")
}

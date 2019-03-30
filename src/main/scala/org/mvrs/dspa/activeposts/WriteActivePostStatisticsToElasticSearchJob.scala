package org.mvrs.dspa.activeposts

import java.util.Properties

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostStatistics
import org.mvrs.dspa.utils


object WriteActivePostStatisticsToElasticSearchJob extends App {

  val elasticHostName = "localhost"
  val elasticPort = 9200
  val elasticScheme = "http"
  val elasticSearchUri = s"$elasticScheme://$elasticHostName:$elasticPort"
  val indexName = "statistics"
  val typeName = "postStatistics"
  val kafkaBrokers = "localhost:9092"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, indexName) // testing: recreate the index
    ActivePostStatisticsIndex.create(client, indexName, typeName)
  }
  finally {
    client.close()
  }

  val props = new Properties()
  props.setProperty("bootstrap.servers", kafkaBrokers)
  props.setProperty("group.id", "test")
  props.setProperty("isolation.level", "read_committed")

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(3)

  val source = utils.createKafkaConsumer("poststatistics", createTypeInformation[PostStatistics], props)

  val stream = env
    .addSource(source)
    .addSink(ActivePostStatisticsIndex.createSink(elasticHostName, elasticPort, elasticScheme, indexName, typeName))

  // execute program
  env.execute("Move post statistics from Kafka to ElasticSearch")
}

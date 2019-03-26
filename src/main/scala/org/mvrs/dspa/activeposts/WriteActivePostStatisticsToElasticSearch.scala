package org.mvrs.dspa.activeposts

import java.util.Properties

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.PostStatistics
import org.mvrs.dspa.utils


object WriteActivePostStatisticsToElasticSearch extends App {
  val elasticSearchUri = "http://localhost:9200"
  val indexName = "statistics"
  val typeName = "postStatistics"
  val kafkaBrokers = "localhost:9092"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, indexName)
    createStatisticsIndex(client, indexName, typeName)
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

  val source = utils.createKafkaConsumer("post_statistics", createTypeInformation[PostStatistics], props)

  val stream = env.addSource(source).addSink(new ElasticSearchStatisticsSinkFunction(elasticSearchUri, indexName, typeName))

  // execute program
  env.execute("Move post statistics from Kafka to ElasticSearch")

  private def createStatisticsIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("postId"),
          intField("replyCount"),
          intField("likeCount"),
          intField("commentCount"),
          intField("distinctUserCount"),
          booleanField("newPost"),
          dateField("timestamp")
        )
      )
    }.await

  }
}

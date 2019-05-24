package org.mvrs.dspa.streams

import org.apache.flink.api.scala._
import org.mvrs.dspa.Settings
import org.mvrs.dspa.model.{LikeEvent, PostEvent, PostStatistics, RawCommentEvent}
import org.mvrs.dspa.utils.avro.AvroUtils._
import org.mvrs.dspa.utils.kafka.{KafkaCluster, KafkaTopic}

/**
  * Static registry of Kafka topics used in the project
  */
object KafkaTopics {
  private val cluster = new KafkaCluster(Settings.config.getString("kafka.brokers"))

  val posts = new KafkaTopic[PostEvent]("mvrs_posts", cluster)
  val comments = new KafkaTopic[RawCommentEvent]("mvrs_comments", cluster)
  val likes = new KafkaTopic[LikeEvent]("mvrs_likes", cluster)
  val postStatistics = new KafkaTopic[PostStatistics]("mvrs_poststatistics", cluster)
}

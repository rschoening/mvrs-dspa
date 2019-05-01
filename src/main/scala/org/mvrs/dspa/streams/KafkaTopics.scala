package org.mvrs.dspa.streams

import org.apache.flink.api.scala._
import org.mvrs.dspa.Settings
import org.mvrs.dspa.model.{CommentEvent, LikeEvent, PostEvent, PostStatistics}
import org.mvrs.dspa.utils.{KafkaCluster, KafkaTopic}

object KafkaTopics {
  private val cluster = new KafkaCluster(Settings.config.getString("kafka.brokers"))

  import org.mvrs.dspa.utils.avro.AvroUtils._

  val posts = new KafkaTopic[PostEvent]("mvrs_posts", cluster)
  val comments = new KafkaTopic[CommentEvent]("mvrs_comments", cluster)
  val likes = new KafkaTopic[LikeEvent]("mvrs_likes", cluster)
  val postStatistics = new KafkaTopic[PostStatistics]("mvrs_poststatistics", cluster)
}

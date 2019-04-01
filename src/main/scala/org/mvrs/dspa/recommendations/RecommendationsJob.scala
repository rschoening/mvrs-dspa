package org.mvrs.dspa.recommendations

import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.ForumEvent
import org.mvrs.dspa.{streams, utils}

import scala.concurrent.ExecutionContext.Implicits.global

object RecommendationsJob extends App {
  val elasticHostName = "localhost"
  val elasticPort = 9200
  val elasticScheme = "http"
  val indexName = "recommendations"
  val typeName = "recommendations_type"
  val elasticSearchUri = s"$elasticScheme://$elasticHostName:$elasticPort"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, indexName) // testing: recreate the index
    RecommendationsIndex.create(client, indexName, typeName)
  }
  finally {
    client.close()
  }

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(5)

  val consumerGroup = "recommendations"
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  val commentsStream = streams.comments(consumerGroup, speedupFactor, randomDelay)
  val postsStream = streams.posts(consumerGroup, speedupFactor, randomDelay)
  val likesStream = streams.likes(consumerGroup, speedupFactor, randomDelay)

  val minHasher = utils.createMinHasher()

  val eventStream =
    commentsStream
      .map(_.asInstanceOf[ForumEvent])
      .union(
        postsStream.map(_.asInstanceOf[ForumEvent]),
        likesStream.map(_.asInstanceOf[ForumEvent]))
      .keyBy(_.personId)

  // FIRST trial: get stored features for person, calculate minhash and buckets, search (asynchronously) for other users in same buckets
  // later: get tags for all posts the user interacted with
  // - either: store post -> tags in elastic
  // - or: broadcast post events to all recommendation operators -> they maintain this mapping

  val minHashes = AsyncDataStream.unorderedWait(
    eventStream.javaStream,
    new AsyncMinHashLookup(elasticSearchUri, minHasher),
    2000L, TimeUnit.MILLISECONDS,
    5)

  // TODO exclude inactive users - keep last activity in this operator? would have to be broadcast to all operators
  //      alternative: keep last activity timestamp in db (both approaches might miss the most recent new activity)
  // TODO allow unit testing with mock function
  val candidates = AsyncDataStream.unorderedWait(
    minHashes,
    new AsyncCandidateUsersLookup(elasticSearchUri, minHasher),
    2000L, TimeUnit.MILLISECONDS,
    5)

  val filteredCandidates = AsyncDataStream.unorderedWait(
    candidates,
    new AsyncFilterCandidates(elasticSearchUri),
    2000L, TimeUnit.MILLISECONDS,
    5)

  val recommendations = AsyncDataStream.unorderedWait(
    filteredCandidates,
    new AsyncRecommendUsers(elasticSearchUri, minHasher),
    2000L, TimeUnit.MILLISECONDS,
    5
  )

  recommendations.addSink(RecommendationsIndex.createSink(elasticHostName, elasticPort, elasticScheme, indexName, typeName))

  env.execute("recommendations")


}








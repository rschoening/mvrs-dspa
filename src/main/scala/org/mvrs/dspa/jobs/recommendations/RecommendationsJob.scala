package org.mvrs.dspa.jobs.recommendations

import java.util.concurrent.TimeUnit

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.events.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams}

import scala.collection.mutable

object RecommendationsJob extends App {
  val recommendationsIndexName = "recommendations"
  val recommendationsTypeName = "recommendations_type"
  val postFeaturesIndexName = "recommendations_posts"
  val postFeaturesTypeName = "recommendations_posts_type"
  val elasticSearchNode = ElasticSearchNode("localhost")

  val recommendationsIndex = new RecommendationsIndex(recommendationsIndexName, recommendationsTypeName, elasticSearchNode)
  recommendationsIndex.create()

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(5)

  val consumerGroup = "recommendations"
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  //  val commentsStream = streams.commentsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val postsStream = streams.postsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val likesStream = streams.likesFromKafka(consumerGroup, speedupFactor, randomDelay)

  val minHasher = RecommendationUtils.createMinHasher()

  val commentsStream: DataStream[CommentEvent] = streams.commentsFromCsv(Settings.commentStreamCsvPath, speedupFactor, randomDelay)
  val postsStream: DataStream[PostEvent] = streams.postsFromCsv(Settings.postStreamCsvPath, speedupFactor, randomDelay)
  val likesStream: DataStream[LikeEvent] = streams.likesFromCsv(Settings.likesStreamCsvPath, speedupFactor, randomDelay)

  // gather features from user activity in sliding window
  val postIds = postsInteractedWith(commentsStream, postsStream, likesStream, Time.hours(4), Time.minutes(60))

  val personActivityFeatures: SingleOutputStreamOperator[(Long, Set[String])] = AsyncDataStream.unorderedWait(
    postIds.javaStream,
    new AsyncPostFeaturesLookup(elasticSearchNode),
    2000L, TimeUnit.MILLISECONDS, 5)

  // calculate minhash for person features
  val personActivityMinHash = personActivityMinHash(personActivityFeatures, minHasher)

  // TODO exclude inactive users - keep last activity in this operator? would have to be broadcast to all operators
  //      alternative: keep last activity timestamp in db (both approaches might miss the most recent new activity)
  // TODO allow unit testing with mock function
  val candidates = AsyncDataStream.unorderedWait(
    personActivityMinHash.javaStream,
    new AsyncCandidateUsersLookup(minHasher, elasticSearchNode),
    2000L, TimeUnit.MILLISECONDS,
    5)

  val filteredCandidates = AsyncDataStream.unorderedWait(
    candidates,
    new AsyncFilterCandidates(elasticSearchNode),
    2000L, TimeUnit.MILLISECONDS,
    5)

  val recommendations = AsyncDataStream.unorderedWait(
    filteredCandidates,
    new AsyncRecommendUsers(minHasher, elasticSearchNode),
    2000L, TimeUnit.MILLISECONDS,
    5
  )

  recommendations.addSink(recommendationsIndex.createSink(batchSize = 100))

  env.execute("recommendations")

  def personActivityMinHash(personActivityFeatures: SingleOutputStreamOperator[(Long, Set[String])], minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] = {
    new DataStream(
      personActivityFeatures
        .map {
          case (personId: Long, features: Set[String]) => (
            personId,
            RecommendationUtils.getMinHashSignature(features.toSeq, minHasher)
          )
        }
        .returns(createTypeInformation[(Long, MinHashSignature)]))
  }

  def postsInteractedWith(comments: DataStream[CommentEvent],
                          posts: DataStream[PostEvent],
                          likes: DataStream[LikeEvent],
                          windowSize: Time,
                          windowSlide: Time): DataStream[(Long, mutable.Set[Long])] = {
    val eventStream =
      comments
        .map(_.asInstanceOf[ForumEvent])
        .union(
          posts.map(_.asInstanceOf[ForumEvent]),
          likes.map(_.asInstanceOf[ForumEvent]))
        .keyBy(_.personId)

    // gather features from user activity in sliding window

    eventStream
      .timeWindow(windowSize, windowSlide)
      .aggregate(new CollectSetFunction[ForumEvent, Long, Long](key = _.personId, value = _.postId))
  }
}

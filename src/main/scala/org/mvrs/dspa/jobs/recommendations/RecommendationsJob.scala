package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.events.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams, utils}

import scala.collection.mutable

object RecommendationsJob extends App {
  val windowSize = Time.hours(4)
  val windowSlide = Time.hours(1)
  val recommendationsIndexName = "recommendations"
  val recommendationsTypeName = "recommendations_type"
  val postFeaturesIndexName = "recommendations_posts"
  val postFeaturesTypeName = "recommendations_posts_type"
  val knownPersonsIndexName = "recommendation_known_persons"
  val knownPersonsTypeName = "recommendation_known_persons_type"
  val lshBucketsIndexName = "recommendation_lsh_buckets"
  val personMinhashIndexName = "recommendation_person_minhash"
  val personFeaturesIndexName = "recommendation_person_features"
  val personFeaturesTypeName = "recommendation_person_features_type"

  val tracedPersonIds: Set[Long] = Set(913L)

  val esNode = ElasticSearchNode("localhost")

  val recommendationsIndex = new RecommendationsIndex(recommendationsIndexName, recommendationsTypeName, esNode)
  recommendationsIndex.create()

  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(localWithUI = false)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(4)

  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  // val consumerGroup = "recommendations"
  //  val commentsStream = streams.commentsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val postsStream = streams.postsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val likesStream = streams.likesFromKafka(consumerGroup, speedupFactor, randomDelay)

  val minHasher = RecommendationUtils.createMinHasher()

  val commentsStream: DataStream[CommentEvent] = streams.commentsFromCsv(Settings.commentStreamCsvPath, speedupFactor, randomDelay)
  val postsStream: DataStream[PostEvent] = streams.postsFromCsv(Settings.postStreamCsvPath, speedupFactor, randomDelay)
  val likesStream: DataStream[LikeEvent] = streams.likesFromCsv(Settings.likesStreamCsvPath, speedupFactor, randomDelay)

  // TODO broadcast last activity date per person id

  // gather the posts that the user interacted with in a sliding window
  val postIds = collectPostsInteractedWith(commentsStream, postsStream, likesStream, windowSize, windowSlide)

  // look up the tags for these posts (from post and from the post's forum)
  val personActivityFeatures: DataStream[(Long, Set[String])] = utils.asyncStream(
    postIds,
    new AsyncPostFeaturesLookup(postFeaturesIndexName, esNode))

  // combine with stored interests of person
  val allPersonFeatures = utils.asyncStream(
    personActivityFeatures, new AsyncUnionWithPersonFeatures(personFeaturesIndexName, personFeaturesTypeName, esNode))

  // calculate minhash for person features
  val personActivityMinHash = getPersonMinHash(personActivityFeatures, minHasher)

  // look up the persons in same lsh buckets
  val candidates = utils.asyncStream(
    personActivityMinHash, new AsyncCandidateUsersLookup(lshBucketsIndexName, minHasher, esNode))

  // exclude already known persons from recommendations
  val filteredCandidates = utils.asyncStream(
    candidates, new AsyncFilterCandidates(knownPersonsIndexName, knownPersonsTypeName, esNode))

  // TODO exclude inactive users - keep last activity in this operator? would have to be broadcast to all operators
  //      alternative: keep last activity timestamp in db (both approaches might miss the most recent new activity)

  val recommendations = utils.asyncStream(
    filteredCandidates, new AsyncRecommendUsers(personMinhashIndexName, minHasher, esNode))

  // debug output for selected person Ids
  if (tracedPersonIds.nonEmpty) {
    postIds.filter(t => tracedPersonIds.contains(t._1)).print("Post Ids:".padTo(12, ' '))
    personActivityFeatures.filter(t => tracedPersonIds.contains(t._1)).print("Activity:".padTo(12, ' '))
    allPersonFeatures.filter(t => tracedPersonIds.contains(t._1)).print("Combined:".padTo(12, ' '))
    candidates.filter(t => tracedPersonIds.contains(t._1)).print("Candidates:".padTo(12, ' '))
    filteredCandidates.filter(t => tracedPersonIds.contains(t._1)).print("Filtered:".padTo(12, ' '))
    recommendations.filter(t => tracedPersonIds.contains(t._1)).print("-> RESULT:".padTo(12, ' '))
  }

  recommendations.addSink(recommendationsIndex.createSink(batchSize = 100))

  env.execute("recommendations")


  def getPersonMinHash(personFeatures: DataStream[(Long, Set[String])],
                       minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] =
    new DataStream(
      personFeatures.javaStream
        .map {
          case (personId: Long, features: Set[String]) => (
            personId,
            RecommendationUtils.getMinHashSignature(features.toSeq, minHasher)
          )
        }
        .returns(createTypeInformation[(Long, MinHashSignature)])
    )

  def collectPostsInteractedWith(comments: DataStream[CommentEvent],
                                 posts: DataStream[PostEvent],
                                 likes: DataStream[LikeEvent],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, mutable.Set[Long])] = {
    val allEvents =
      comments
        .map(_.asInstanceOf[ForumEvent])
        .union(
          posts.map(_.asInstanceOf[ForumEvent]),
          likes.map(_.asInstanceOf[ForumEvent]))
        .keyBy(_.personId)

    // gather features from user activity in sliding window

    allEvents
      .timeWindow(windowSize, windowSlide)
      .aggregate(new CollectSetFunction[ForumEvent, Long, Long](key = _.personId, value = _.postId))
  }
}

package org.mvrs.dspa.jobs.recommendations

import java.lang
import java.util.concurrent.TimeUnit

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.elastic.ElasticSearchNode
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

import scala.collection.JavaConverters._

object RecommendationsJob extends FlinkStreamingJob {
  val windowSize = Time.milliseconds(Settings.config.getDuration("jobs.recommendation.activity-window-size", TimeUnit.MILLISECONDS))
  val windowSlide = Time.milliseconds(Settings.config.getDuration("jobs.recommendation.activity-window-slide", TimeUnit.MILLISECONDS))
  val activeUsersTimeout = Time.milliseconds(Settings.config.getDuration("jobs.recommendation.active-users-timeout", TimeUnit.MILLISECONDS))
  val tracedPersonIds: Set[lang.Long] = Settings.config.getLongList("jobs.recommendation.trace-person-ids").asScala.toSet

  implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
  implicit val minHasher: MinHasher32 = RecommendationUtils.minHasher

  // (re)create the index for storing recommendations (person-id -> List((person-id, similarity))
  ElasticSearchIndexes.recommendations.create()

  // val kafkaConsumerGroup = Some("recommendations")
  val commentsStream: DataStream[CommentEvent] = streams.comments()
  val postsStream: DataStream[PostEvent] = streams.posts()
  val likesStream: DataStream[LikeEvent] = streams.likes()

  val forumEvents: DataStream[ForumEvent] = unionEvents(commentsStream, postsStream, likesStream)

  // gather the posts that the user interacted with in a sliding window
  val postIds: DataStream[(Long, Set[Long])] = collectPostsInteractedWith(forumEvents, windowSize, windowSlide)

  // look up the tags for these posts (from post and from the post's forum) => (person id -> set of features)
  val personActivityFeatures: DataStream[(Long, Set[String])] = lookupPostFeaturesForPerson(postIds)

  // combine with stored interests of person (person id -> set of features)
  val allPersonFeatures: DataStream[(Long, Set[String])] = unionWithPersonFeatures(personActivityFeatures)

  // calculate minhash per person id, based on person features
  val personActivityMinHash: DataStream[(Long, MinHashSignature)] = getPersonMinHash(personActivityFeatures)

  // look up the persons in same lsh buckets
  val candidates: DataStream[(Long, MinHashSignature, Set[Long])] = lookupCandidateUsers(personActivityMinHash)

  // exclude already known persons from recommendations
  val candidatesWithoutKnownPersons: DataStream[(Long, MinHashSignature, Set[Long])] = excludeKnownPersons(candidates)

  // exclude inactive users from recommendations
  val candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])] =
    filterToActiveUsers(candidatesWithoutKnownPersons, forumEvents, activeUsersTimeout)


  val recommendations: DataStream[(Long, Seq[(Long, Double)])] =
    recommendUsers(
      candidatesWithoutInactiveUsers,
      Settings.config.getInt("jobs.recommendation.max-recommendation-count"),
      Settings.config.getInt("jobs.recommendation.min-recommendation-similarity"),
    )

  tracePersons(tracedPersonIds)

  recommendations.addSink(ElasticSearchIndexes.recommendations.createSink(batchSize = 100))

  env.execute("recommendations")

  private def tracePersons(personIds: Set[lang.Long]) = {
    // debug output for selected person Ids
    if (personIds.nonEmpty) {
      val length = 15
      postIds.filter(t => personIds.contains(t._1)).print("Post Ids:".padTo(length, ' '))
      personActivityFeatures.filter(t => personIds.contains(t._1)).print("Activity:".padTo(length, ' '))
      allPersonFeatures.filter(t => personIds.contains(t._1)).print("Combined:".padTo(length, ' '))
      candidates.filter(t => personIds.contains(t._1)).print("Candidates:".padTo(length, ' '))
      candidatesWithoutKnownPersons.filter(t => personIds.contains(t._1)).print("Filtered:".padTo(length, ' '))
      candidatesWithoutInactiveUsers.filter(t => personIds.contains(t._1)).print("Active only:".padTo(length, ' '))
      recommendations.filter(t => personIds.contains(t._1)).print("-> RESULT:".padTo(length, ' '))
    }
  }

  def lookupPostFeaturesForPerson(postIds: DataStream[(Long, Set[Long])])
                                 (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] = {
    FlinkUtils.asyncStream(
      postIds,
      new AsyncPostFeaturesLookupFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        esNodes: _*))
  }

  def unionWithPersonFeatures(personActivityFeatures: DataStream[(Long, Set[String])])
                             (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] = {
    FlinkUtils.asyncStream(
      personActivityFeatures,
      new AsyncUnionWithPersonFeaturesFunction(
        ElasticSearchIndexes.personFeatures.indexName,
        ElasticSearchIndexes.personFeatures.typeName,
        esNodes: _*))
  }

  def getPersonMinHash(personFeatures: DataStream[(Long, Set[String])])
                      (implicit minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, RecommendationUtils.getMinHashSignature(t._2, minHasher)))

  def unionEvents(comments: DataStream[CommentEvent],
                  posts: DataStream[PostEvent],
                  likes: DataStream[LikeEvent]): DataStream[ForumEvent] =
    comments
      .map(_.asInstanceOf[ForumEvent])
      .union(
        posts.map(_.asInstanceOf[ForumEvent]),
        likes.map(_.asInstanceOf[ForumEvent]))

  def filterToActiveUsers(candidates: DataStream[(Long, MinHashSignature, Set[Long])],
                          forumEvents: DataStream[ForumEvent],
                          activityTimeout: Time) = {
    val stateDescriptor = createActiveUsersStateDescriptor(activityTimeout)

    val broadcastActivePersons =
      forumEvents
        .map(_.personId)
        .broadcast(stateDescriptor)

    candidatesWithoutKnownPersons
      .connect(broadcastActivePersons)
      .process(new FilterToActivePersonsFunction(activityTimeout, stateDescriptor))
  }

  def collectPostsInteractedWith(forumEvents: DataStream[ForumEvent],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, Set[Long])] = {
    // gather features from user activity in sliding window
    forumEvents
      .keyBy(_.personId)
      .timeWindow(
        size = FlinkUtils.convert(windowSize),
        slide = FlinkUtils.convert(windowSlide))
      .aggregate(new CollectSetFunction[ForumEvent, Long, Long](
        key = _.personId,
        value = _.postId))
  }

  def createActiveUsersStateDescriptor(timeout: Time): MapStateDescriptor[Long, Long] = {
    val ttlConfig = StateTtlConfig
      .newBuilder(timeout)
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val descriptor: MapStateDescriptor[Long, Long] = new MapStateDescriptor(
      "active-users",
      createTypeInformation[Long],
      createTypeInformation[Long])

    descriptor.enableTimeToLive(ttlConfig)

    descriptor
  }

  def lookupCandidateUsers(personActivityMinHash: DataStream[(Long, MinHashSignature)])
                          (implicit esNodes: Seq[ElasticSearchNode], minHasher: MinHasher32): DataStream[(Long, MinHashSignature, Set[Long])] = {
    FlinkUtils.asyncStream(
      personActivityMinHash,
      new AsyncCandidateUsersLookupFunction(
        ElasticSearchIndexes.lshBuckets.indexName,
        minHasher, esNodes: _*))
  }

  def excludeKnownPersons(candidates: DataStream[(Long, MinHashSignature, Set[Long])])
                         (implicit esNodes: Seq[ElasticSearchNode]) = {
    FlinkUtils.asyncStream(
      candidates,
      new AsyncExcludeKnownPersonsFunction(
        ElasticSearchIndexes.knownPersons.indexName,
        ElasticSearchIndexes.knownPersons.typeName,
        esNodes: _*))
  }

  def recommendUsers(candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])], maxCount: Int, minSimilarity: Double)
                    (implicit esNodes: Seq[ElasticSearchNode], minHasher: MinHasher32) = {
    FlinkUtils.asyncStream(candidatesWithoutInactiveUsers,
      new AsyncRecommendUsersFunction(
        ElasticSearchIndexes.personMinHashes.indexName,
        minHasher, maxCount, minSimilarity,
        esNodes: _*))
  }

}


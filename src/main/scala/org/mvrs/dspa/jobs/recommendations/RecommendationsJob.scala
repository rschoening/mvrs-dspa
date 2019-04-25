package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

object RecommendationsJob extends FlinkStreamingJob {
  val windowSize = Time.hours(4)
  val windowSlide = Time.hours(1)
  val activeUsersTimeout = Time.days(14)

  val tracedPersonIds: Set[Long] = Set(913L)

  ElasticSearchIndexes.recommendations.create()

  // val consumerGroup = "recommendations"
  //  val commentsStream = streams.commentsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val postsStream = streams.postsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val likesStream = streams.likesFromKafka(consumerGroup, speedupFactor, randomDelay)

  val commentsStream: DataStream[CommentEvent] = streams.comments()
  val postsStream: DataStream[PostEvent] = streams.posts()
  val likesStream: DataStream[LikeEvent] = streams.likes()

  val forumEvents: DataStream[ForumEvent] = unionForumEvents(
    commentsStream,
    postsStream,
    likesStream
  )

  // gather the posts that the user interacted with in a sliding window
  val postIds: DataStream[(Long, Set[Long])] =
    collectPostsInteractedWith(forumEvents, windowSize, windowSlide)

  // look up the tags for these posts (from post and from the post's forum) => (person id -> set of features)
  val personActivityFeatures: DataStream[(Long, Set[String])] =
    FlinkUtils.asyncStream(
      postIds,
      new AsyncPostFeaturesLookupFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        Settings.elasticSearchNodes: _*))

  // combine with stored interests of person (person id -> set of features)
  val allPersonFeatures: DataStream[(Long, Set[String])] =
    FlinkUtils.asyncStream(
      personActivityFeatures,
      new AsyncUnionWithPersonFeaturesFunction(
        ElasticSearchIndexes.personFeatures.indexName,
        ElasticSearchIndexes.personFeatures.typeName,
        Settings.elasticSearchNodes: _*))

  // calculate minhash per person id, based on person features
  val personActivityMinHash: DataStream[(Long, MinHashSignature)] =
    getPersonMinHash(personActivityFeatures, RecommendationUtils.minHasher)

  // look up the persons in same lsh buckets
  val candidates: DataStream[(Long, MinHashSignature, Set[Long])] =
    FlinkUtils.asyncStream(
      personActivityMinHash,
      new AsyncCandidateUsersLookupFunction(
        ElasticSearchIndexes.lshBuckets.indexName,
        RecommendationUtils.minHasher,
        Settings.elasticSearchNodes: _*))

  // exclude already known persons from recommendations
  val candidatesWithoutKnownPersons: DataStream[(Long, MinHashSignature, Set[Long])] =
    FlinkUtils.asyncStream(
      candidates,
      new AsyncExcludeKnownPersonsFunction(
        ElasticSearchIndexes.knownPersons.indexName,
        ElasticSearchIndexes.knownPersons.typeName,
        Settings.elasticSearchNodes: _*))

  val candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])] =
    filterToActiveUsers(candidatesWithoutKnownPersons, forumEvents, activeUsersTimeout)

  val recommendations = FlinkUtils.asyncStream(
    candidatesWithoutInactiveUsers,
    new AsyncRecommendUsersFunction(
      ElasticSearchIndexes.personMinHashes.indexName,
      RecommendationUtils.minHasher,
      Settings.config.getInt("jobs.recommendation.max-recommendation-count"),
      Settings.config.getInt("jobs.recommendation.min-recommendation-similarity"),
      Settings.elasticSearchNodes: _*))

  // debug output for selected person Ids
  if (tracedPersonIds.nonEmpty) {
    val length = 15
    postIds.filter(t => tracedPersonIds.contains(t._1)).print("Post Ids:".padTo(length, ' '))
    personActivityFeatures.filter(t => tracedPersonIds.contains(t._1)).print("Activity:".padTo(length, ' '))
    allPersonFeatures.filter(t => tracedPersonIds.contains(t._1)).print("Combined:".padTo(length, ' '))
    candidates.filter(t => tracedPersonIds.contains(t._1)).print("Candidates:".padTo(length, ' '))
    candidatesWithoutKnownPersons.filter(t => tracedPersonIds.contains(t._1)).print("Filtered:".padTo(length, ' '))
    candidatesWithoutInactiveUsers.filter(t => tracedPersonIds.contains(t._1)).print("Active only:".padTo(length, ' '))
    recommendations.filter(t => tracedPersonIds.contains(t._1)).print("-> RESULT:".padTo(length, ' '))
  }

  recommendations.addSink(ElasticSearchIndexes.recommendations.createSink(batchSize = 100))

  env.execute("recommendations")

  def getPersonMinHash(personFeatures: DataStream[(Long, Set[String])],
                       minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, RecommendationUtils.getMinHashSignature(t._2, minHasher)))

  def unionForumEvents(comments: DataStream[CommentEvent],
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

  private def createActiveUsersStateDescriptor(timeout: Time) = {
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
}


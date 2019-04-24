package org.mvrs.dspa.jobs.recommendations

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.mvrs.dspa.events.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.{Settings, streams, utils}

object RecommendationsJob extends App {
  val windowSize = Time.hours(4)
  val windowSlide = Time.hours(1)
  val activeUsersTimeout = Time.days(14)
  val minimumRecommendationSimilarity = 0.1
  val maximumRecommendationCount = 5

  val localWithUI = false
  val tracedPersonIds: Set[Long] = Set(913L)

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

  val esNode = ElasticSearchNode("localhost")

  val recommendationsIndex = new RecommendationsIndex(recommendationsIndexName, recommendationsTypeName, Settings.elasticSearchNodes(): _*)
  recommendationsIndex.create()

  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(localWithUI)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(4)

  val minHasher = RecommendationUtils.createMinHasher()

  // val consumerGroup = "recommendations"
  //  val commentsStream = streams.commentsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val postsStream = streams.postsFromKafka(consumerGroup, speedupFactor, randomDelay)
  //  val likesStream = streams.likesFromKafka(consumerGroup, speedupFactor, randomDelay)

  val commentsStream: DataStream[CommentEvent] = streams.comments()
  val postsStream: DataStream[PostEvent] = streams.posts()
  val likesStream: DataStream[LikeEvent] = streams.likes()

  val forumEvents: DataStream[ForumEvent] = unionForumEvents(commentsStream, postsStream, likesStream)

  // gather the posts that the user interacted with in a sliding window
  val postIds: DataStream[(Long, Set[Long])] =
    collectPostsInteractedWith(forumEvents, windowSize, windowSlide)

  // look up the tags for these posts (from post and from the post's forum) => (person id -> set of features)
  val personActivityFeatures: DataStream[(Long, Set[String])] =
    utils.asyncStream(postIds,
      new AsyncPostFeaturesLookupFunction(postFeaturesIndexName, esNode))

  // combine with stored interests of person (person id -> set of features)
  val allPersonFeatures: DataStream[(Long, Set[String])] =
    utils.asyncStream(personActivityFeatures,
      new AsyncUnionWithPersonFeaturesFunction(personFeaturesIndexName, personFeaturesTypeName, esNode))

  // calculate minhash per person id, based on person features
  val personActivityMinHash: DataStream[(Long, MinHashSignature)] =
    getPersonMinHash(personActivityFeatures, minHasher)

  // look up the persons in same lsh buckets
  val candidates: DataStream[(Long, MinHashSignature, Set[Long])] =
    utils.asyncStream(personActivityMinHash,
      new AsyncCandidateUsersLookupFunction(lshBucketsIndexName, minHasher, esNode))

  // exclude already known persons from recommendations
  val candidatesWithoutKnownPersons: DataStream[(Long, MinHashSignature, Set[Long])] =
    utils.asyncStream(candidates,
      new AsyncExcludeKnownPersonsFunction(knownPersonsIndexName, knownPersonsTypeName, esNode))

  val candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])] =
    filterToActiveUsers(candidatesWithoutKnownPersons, forumEvents, activeUsersTimeout)

  val recommendations = utils.asyncStream(
    candidatesWithoutInactiveUsers, new AsyncRecommendUsersFunction(
      personMinhashIndexName, minHasher,
      maximumRecommendationCount, minimumRecommendationSimilarity,
      esNode))

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

  recommendations.addSink(recommendationsIndex.createSink(batchSize = 100))

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
        size = utils.convert(windowSize),
        slide = utils.convert(windowSlide))
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


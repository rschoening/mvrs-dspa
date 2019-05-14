package org.mvrs.dspa.jobs.recommendations

import java.lang

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.functions.CollectSetFunction
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model.{CommentEvent, LikeEvent, PostEvent, PostFeatures}
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

import scala.collection.JavaConverters._

object RecommendationsJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): Unit = {
    // read settings
    val windowSize = Settings.duration("jobs.recommendation.activity-window-size")
    val windowSlide = Settings.duration("jobs.recommendation.activity-window-slide")
    val activeUsersTimeout = Settings.duration("jobs.recommendation.active-users-timeout")
    val tracedPersonIds = Settings.config.getLongList("jobs.recommendation.trace-person-ids").asScala.toSet
    val maxRecommendationCount = Settings.config.getInt("jobs.recommendation.max-recommendation-count")
    val minRecommendationSimilarity = Settings.config.getInt("jobs.recommendation.min-recommendation-similarity")

    // implicit values
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    implicit val minHasher: MinHasher32 = RecommendationUtils.minHasher

    // (re)create the indexes for post features and for storing recommendations (person-id -> List((person-id, similarity))
    ElasticSearchIndexes.postFeatures.create()
    ElasticSearchIndexes.recommendations.create()

    // consume post stream for writing post features, with separate consumer group (to avoid back pressure on main recommendation)
    val postFeaturesStream: DataStream[PostEvent] = streams.posts(Some("recommendations-post-features"))

    // look up forum features for posts
    val postsWithForumFeatures: DataStream[(PostEvent, String, Set[String])] = lookupForumFeatures(postFeaturesStream)

    val postFeatures =
      postsWithForumFeatures
        .map(createPostRecord _)
        .name("Map: -> post event with post and forum features")

    // read input streams
    val kafkaConsumerGroup: Option[String] = Some("recommendations") // None for csv
    val commentsStream: DataStream[CommentEvent] = streams.comments(kafkaConsumerGroup)
    val postsStream: DataStream[PostEvent] = streams.posts(kafkaConsumerGroup)
    val likesStream: DataStream[LikeEvent] = streams.likes(kafkaConsumerGroup)

    // union all streams for all event types
    val forumEvents: DataStream[(Long, Long)] = unionEvents(commentsStream, postsStream, likesStream)

    // gather the posts that the user interacted with in a sliding window
    val postIds: DataStream[(Long, Set[Long])] = collectPostsInteractedWith(forumEvents, windowSize, windowSlide)

    // TODO carry along window timestamps

    // look up the tags for these posts (from post and from the post's forum) => (person id -> set of features)
    val personActivityFeatures: DataStream[(Long, Set[String])] = lookupPostFeaturesForPerson(postIds)

    // combine with stored interests of person (person id -> set of features)
    val allPersonFeatures: DataStream[(Long, Set[String])] = unionWithPersonFeatures(personActivityFeatures)

    // calculate minhash per person id, based on person features
    val personActivityMinHash: DataStream[(Long, MinHashSignature)] = getPersonMinHash(allPersonFeatures)

    // look up the persons in same lsh buckets
    val candidates: DataStream[(Long, MinHashSignature, Set[Long])] = lookupCandidateUsers(personActivityMinHash)

    // exclude already known persons from recommendations
    val candidatesWithoutKnownPersons: DataStream[(Long, MinHashSignature, Set[Long])] = excludeKnownPersons(candidates)

    // exclude inactive users from recommendations
    val candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])] =
      filterToActiveUsers(candidatesWithoutKnownPersons, forumEvents, activeUsersTimeout)

    // calculate recommendation
    val recommendations: DataStream[(Long, Seq[(Long, Double)])] =
      recommendUsers(candidatesWithoutInactiveUsers, maxRecommendationCount, minRecommendationSimilarity)

    // trace the configured persons (output printed to console)
    tracePersons(tracedPersonIds)

    // add sinks
    postFeatures
      .addSink(ElasticSearchIndexes.postFeatures.createSink(10))
      .name("ElasticSearch: post features")

    recommendations
      .addSink(ElasticSearchIndexes.recommendations.createSink(batchSize = 100))
      .name("ElasticSearch: recommendations")

    env.execute("recommendations")

    def tracePersons(personIds: Set[lang.Long]) = {
      // debug output for selected person Ids

      // NOTE due to the unordered Async I/O results, the output is out-of-order even for a single person
      // NOTE it *seems* that disableChaining() calls alter the output - needs further investigation. For now: don't do it

      if (personIds.nonEmpty) {
        val length = 40
        postIds
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("1) Activity in window - post Ids:".padTo(length, ' '))
          .name("print")

        personActivityFeatures
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("2) Activity in window - post features".padTo(length, ' '))
          .name("print")

        allPersonFeatures
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("3) Combined person features:".padTo(length, ' '))
          .name("print")

        candidates
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("4) Candidates from same LSH buckets:".padTo(length, ' '))
          .name("print")

        candidatesWithoutKnownPersons
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("5) Candidates without known persons:".padTo(length, ' '))
          .name("print")

        candidatesWithoutInactiveUsers
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("6) Candidates except inactive:".padTo(length, ' '))
          .name("print")

        recommendations
          .filter(t => personIds.contains(t._1))
          .print("-> Resulting recommendation:".padTo(length, ' '))
          .name("print")
      }
    }
  }

  def lookupForumFeatures(postsStream: DataStream[PostEvent]): DataStream[(PostEvent, String, Set[String])] = {
    FlinkUtils.asyncStream(
      postsStream,
      new AsyncForumLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        Settings.elasticSearchNodes: _*))
      .name("Async I/O: look up forum features")
      .startNewChain()
  }

  def createPostRecord(t: (PostEvent, String, Set[String])): PostFeatures = {
    val postEvent = t._1
    val forumTitle = t._2
    val forumFeatures = t._3

    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.forumId,
      forumTitle,
      postEvent.timestamp,
      postEvent.content.getOrElse(""),
      postEvent.imageFile.getOrElse(""),
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, FeaturePrefix.Tag))
    )
  }

  def lookupPostFeaturesForPerson(postIds: DataStream[(Long, Set[Long])])
                                 (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] = {
    FlinkUtils.asyncStream(
      postIds,
      new AsyncPostFeaturesLookupFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        esNodes: _*))
      .name("Async I/O: look up post features")
  }

  def unionWithPersonFeatures(personActivityFeatures: DataStream[(Long, Set[String])])
                             (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] = {
    FlinkUtils.asyncStream(
      personActivityFeatures,
      new AsyncUnionWithPersonFeaturesFunction(
        ElasticSearchIndexes.personFeatures.indexName,
        ElasticSearchIndexes.personFeatures.typeName,
        esNodes: _*))
      .name("Async I/O: union with person features")
  }

  def getPersonMinHash(personFeatures: DataStream[(Long, Set[String])])
                      (implicit minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] =
    personFeatures
      .map(t => (
        t._1, // person Id
        RecommendationUtils.getMinHashSignature(t._2, minHasher)) // MinHash signature
      )
      .name("Map: calculate MinHash signature for person")

  def unionEvents(comments: DataStream[CommentEvent],
                  posts: DataStream[PostEvent],
                  likes: DataStream[LikeEvent]): DataStream[(Long, Long)] =
    comments
      .map(c => (c.personId, c.postId)).name("Map: -> (person Id, post Id)")
      .union(
        posts.map(p => (p.personId, p.postId)).name("Map: -> (person Id, post Id)"),
        likes.map(l => (l.personId, l.postId)).name("Map: -> (person Id, post Id)")
      )

  def filterToActiveUsers(candidates: DataStream[(Long, MinHashSignature, Set[Long])],
                          forumEvents: DataStream[(Long, Long)],
                          activityTimeout: Time) = {
    val stateDescriptor = createActiveUsersStateDescriptor(activityTimeout)

    val broadcastActivePersons =
      forumEvents
        .map(_._1)
        .name("Map: -> person Id")
        .broadcast(stateDescriptor)

    candidates
      .connect(broadcastActivePersons)
      .process(new FilterToActivePersonsFunction(activityTimeout, stateDescriptor))
      .name("Filter to active persons")
  }

  def collectPostsInteractedWith(forumEvents: DataStream[(Long, Long)],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, Set[Long])] = {
    // gather features from user activity in sliding window
    forumEvents
      .keyBy(_._1) // person id
      .timeWindow(size = FlinkUtils.convert(windowSize), slide = FlinkUtils.convert(windowSlide))
      .aggregate(new CollectSetFunction[(Long, Long), Long, Long](
        key = _._1,
        value = _._2))
      .name("Collect posts with which the person interacted " +
        s"(window: ${DateTimeUtils.formatDuration(windowSize.toMilliseconds)}" +
        s"slide ${DateTimeUtils.formatDuration(windowSlide.toMilliseconds)})")
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
                          (implicit esNodes: Seq[ElasticSearchNode],
                           minHasher: MinHasher32): DataStream[(Long, MinHashSignature, Set[Long])] = {
    FlinkUtils.asyncStream(
      personActivityMinHash,
      new AsyncCandidateUsersLookupFunction(
        ElasticSearchIndexes.lshBuckets.indexName,
        minHasher, esNodes: _*))
      .name("Async I/O: get candidates in same LSH buckets")
      .startNewChain()
  }

  def excludeKnownPersons(candidates: DataStream[(Long, MinHashSignature, Set[Long])])
                         (implicit esNodes: Seq[ElasticSearchNode]) = {
    FlinkUtils.asyncStream(
      candidates,
      new AsyncExcludeKnownPersonsFunction(
        ElasticSearchIndexes.knownPersons.indexName,
        ElasticSearchIndexes.knownPersons.typeName,
        esNodes: _*))
      .name("Async I/O: exclude known persons from candidates")
      .startNewChain()
  }

  def recommendUsers(candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])], maxCount: Int, minSimilarity: Double)
                    (implicit esNodes: Seq[ElasticSearchNode],
                     minHasher: MinHasher32) = {
    FlinkUtils.asyncStream(
      candidatesWithoutInactiveUsers,
      new AsyncRecommendUsersFunction(
        ElasticSearchIndexes.personMinHashes.indexName,
        minHasher, maxCount, minSimilarity,
        esNodes: _*))
      .name("Async I/O: sort and filter candidates based on their MinHash-approximated similarity")
      .startNewChain()
  }
}


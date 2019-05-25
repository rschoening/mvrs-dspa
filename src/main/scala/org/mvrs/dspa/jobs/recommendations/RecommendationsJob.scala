package org.mvrs.dspa.jobs.recommendations

import java.lang

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.common.JobExecutionResult
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

/**
  * Streaming job for finding similar users based on user activity, and writing recommendations to
  * ElasticSearch (DSPA Task #2)
  */
object RecommendationsJob extends FlinkStreamingJob(enableGenericTypes = true) {
  def execute(): JobExecutionResult = {
    // read settings
    val windowSize = Settings.duration("jobs.recommendation.activity-window-size")
    val windowSlide = Settings.duration("jobs.recommendation.activity-window-slide")
    val activeUsersTimeout = Settings.duration("jobs.recommendation.active-users-timeout")
    val tracedPersonIds = Settings.config.getLongList("jobs.recommendation.trace-person-ids").asScala.toSet
    val maxRecommendationCount = Settings.config.getInt("jobs.recommendation.max-recommendation-count")
    val minRecommendationSimilarity = Settings.config.getInt("jobs.recommendation.min-recommendation-similarity")
    val postFeaturesBatchSize = Settings.config.getInt("jobs.recommendation.post-features-elasticsearch-batch-size")
    val recommendationsBatchSize = Settings.config.getInt("jobs.recommendation.recommendations-elasticsearch-batch-size")

    // implicit values
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes
    implicit val minHasher: MinHasher32 = RecommendationUtils.minHasher

    // (re)create the indexes for post features and recommendations
    ElasticSearchIndexes.postFeatures.create()
    ElasticSearchIndexes.recommendations.create() // person-id -> List[(person-id, similarity)]

    // ---------------------------------------------------------------------------------------------
    // Pipeline 1: read post events, look up forum features, write combined post features to Kafka
    // ---------------------------------------------------------------------------------------------

    // consume post stream from Kafka, for writing post features (-> separate consumer group)
    val postFeaturesStream: DataStream[PostEvent] = streams.posts(Some("recommendations-post-features"))

    // look up forum features for posts
    val postsWithForumFeatures: DataStream[(PostEvent, Set[String])] = lookupForumFeatures(postFeaturesStream)

    // Potential optimization: map to reduced post representation *before* the async call, to save
    // space when storing the events with pending responses.

    val postFeatures: DataStream[PostFeatures] =
      postsWithForumFeatures
        .map(t => createPostRecord(t._1, t._2))
        .name("Map: -> post event with post and forum features")

    // add sink
    postFeatures
      .addSink(ElasticSearchIndexes.postFeatures.createSink(postFeaturesBatchSize))
      .name("ElasticSearch: post features")


    // ---------------------------------------------------------------------------------------------------
    // Pipeline 2: read all event types and produce recommendations based on per-person activity in window
    // ---------------------------------------------------------------------------------------------------

    // read input streams from Kafka
    val kafkaConsumerGroup: Option[String] = Some("recommendations")
    val commentsStream: DataStream[CommentEvent] = streams.comments(kafkaConsumerGroup)
    val postsStream: DataStream[PostEvent] = streams.posts(kafkaConsumerGroup)
    val likesStream: DataStream[LikeEvent] = streams.likes(kafkaConsumerGroup)

    // union all streams for all event types as (person id, post id) tuples
    val forumEvents: DataStream[(Long, Long)] = unionEvents(commentsStream, postsStream, likesStream)

    // gather the posts that the user interacted with in a sliding window
    val postIds: DataStream[(Long, Set[Long])] = collectPostsInteractedWith(forumEvents, windowSize, windowSlide)

    // look up the features for these posts (from post and from the post's forum, written by pipeline 1)
    // => (person id -> set of features)
    val personActivityFeatures: DataStream[(Long, Set[String])] = lookupPostFeaturesForPerson(postIds)

    // combine with stored interests of person (person id -> set of features)
    val allPersonFeatures: DataStream[(Long, Set[String])] = unionWithPersonFeatures(personActivityFeatures)

    // calculate minhash per person id, based on person features
    val personActivityMinHash: DataStream[(Long, MinHashSignature)] = getPersonMinHash(allPersonFeatures)

    // look up the persons in same LSH buckets
    val candidates: DataStream[(Long, MinHashSignature, Set[Long])] = lookupCandidateUsers(personActivityMinHash)

    // exclude already known persons from recommendations
    val candidatesWithoutKnownPersons: DataStream[(Long, MinHashSignature, Set[Long])] = excludeKnownPersons(candidates)

    // exclude inactive users from recommendations
    val candidatesWithoutInactiveUsers: DataStream[(Long, MinHashSignature, Set[Long])] =
      filterToActiveUsers(candidatesWithoutKnownPersons, forumEvents, activeUsersTimeout)

    // calculate recommendation
    val recommendations: DataStream[(Long, Seq[(Long, Double)])] =
      recommendUsers(candidatesWithoutInactiveUsers, maxRecommendationCount, minRecommendationSimilarity)

    // add sink
    recommendations
      .addSink(ElasticSearchIndexes.recommendations.createSink(recommendationsBatchSize))
      .name("ElasticSearch: recommendations")

    // example for how to monitor progress at a specific point in the pipeline
    FlinkUtils.addProgressMonitor(candidatesWithoutInactiveUsers) { case (_, progressInfo) => progressInfo.subtask == 0 && true || progressInfo.isLate }

    // trace the configured persons (output printed to console)
    tracePersons(tracedPersonIds)

    /**
      * Print trace information on the individual recommendation steps for a selected set of person ids, which can be
      * defined in the configuration file (`jobs.recommendation.trace-person-ids`)
      *
      * @param personIds the person ids to trace
      */
    def tracePersons(personIds: Set[lang.Long]): Unit = {
      // debug output for selected person Ids

      // NOTE due to the unordered Async I/O results, the output is out-of-order even for a single person
      // NOTE it *seems* that disableChaining() calls alter the output - needs further investigation. For now: don't do it

      if (personIds.nonEmpty) {
        val padLength = 40
        postIds
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("1) Activity in window - post Ids:".padTo(padLength, ' '))
          .name("print")

        personActivityFeatures
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("2) Activity in window - post features".padTo(padLength, ' '))
          .name("print")

        allPersonFeatures
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("3) Combined person features:".padTo(padLength, ' '))
          .name("print")

        candidates
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("4) Candidates from same LSH buckets:".padTo(padLength, ' '))
          .name("print")

        candidatesWithoutKnownPersons
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("5) Candidates without known persons:".padTo(padLength, ' '))
          .name("print")

        candidatesWithoutInactiveUsers
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("6) Candidates except inactive:".padTo(padLength, ' '))
          .name("print")

        recommendations
          .filter(t => personIds.contains(t._1))
          .name("Filter: trace persons")
          .print("-> Resulting recommendation:".padTo(padLength, ' '))
          .name("print")
      }
    }

    FlinkUtils.printExecutionPlan()

    env.execute("recommendations")
  }

  /**
    * Looks up forum features from elastic search, for the posts in an input stream
    *
    * @param postsStream The stream of post events
    * @return Stream of post events with looked up sets of forum features
    */
  def lookupForumFeatures(postsStream: DataStream[PostEvent]): DataStream[(PostEvent, Set[String])] =
    FlinkUtils.asyncStream(
      postsStream,
      new AsyncForumFeaturesLookupFunction(
        ElasticSearchIndexes.forumFeatures.indexName,
        Settings.elasticSearchNodes: _*))
      .name("Async I/O: look up forum features")

  /**
    * Create post record with feature information from both the post event and the forum event in which
    * the post occurred.
    *
    * @param postEvent     : The post event, with collection of tags
    * @param forumFeatures : The set of forum features (derived from forum tags)
    * @return Record with the required post information (post id, person id, and timestamp) and the
    *         unioned feature sets of post and forum
    */
  def createPostRecord(postEvent: PostEvent, forumFeatures: Set[String]): PostFeatures =
    PostFeatures(
      postEvent.postId,
      postEvent.personId,
      postEvent.timestamp,
      forumFeatures ++ postEvent.tags.map(RecommendationUtils.toFeature(_, FeaturePrefix.Tag))
    )

  /**
    * For a stream of persons with associated post ids, looks up the set of features for these posts, and
    * outputs a stream of (person id, Set[feature]) tuples.
    *
    * @param postIds Input stream of (person id, Set[post id]) tuples
    * @param esNodes The ElasticSearch nodes to connect to
    * @return Stream of (person id, Set[feature]) tuples.
    */
  def lookupPostFeaturesForPerson(postIds: DataStream[(Long, Set[Long])])
                                 (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] =
    FlinkUtils.asyncStream(
      postIds,
      new AsyncPostFeaturesLookupFunction(
        ElasticSearchIndexes.postFeatures.indexName,
        esNodes: _*))
      .name("Async I/O: look up post features")

  /**
    * For a stream of person ids with associated activity-based features (post interaction), statically defined
    * person features are looked up and unioned with the activity features to produce an output stream of
    * (person id, Set[feature]) tuples.
    *
    * @param personActivityFeatures Input stream of (person id, Set[feature]) tuples
    * @param esNodes                The ElasticSearch nodes to connect to
    * @return Stream of (person id, Set[feature]) tuples, where the feature set consists of both activity-based and
    *         static features
    */
  def unionWithPersonFeatures(personActivityFeatures: DataStream[(Long, Set[String])])
                             (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, Set[String])] =
    FlinkUtils.asyncStream(
      personActivityFeatures,
      new AsyncUnionWithPersonFeaturesFunction(
        ElasticSearchIndexes.personFeatures.indexName,
        ElasticSearchIndexes.personFeatures.typeName,
        esNodes: _*))
      .name("Async I/O: union with person features")

  /**
    * For a stream of per-person features, calculates the MinHash signature based on these features and produces a
    * stream of (person id, MinHash-signature) tuples.
    *
    * @param personFeatures Input stream of (person id, Set[feature]) tuples
    * @param minHasher      The MinHasher to calculate the signatures.
    * @return Stream of (person id, MinHash-signatuure) tuples
    * @note The same MinHash parameters (hash functions, LSH bands) must be used as for the bucket assignment of all
    *       persons based on their static properties (in the batch job of static data preparation)
    */
  def getPersonMinHash(personFeatures: DataStream[(Long, Set[String])])
                      (implicit minHasher: MinHasher32): DataStream[(Long, MinHashSignature)] =
    personFeatures
      .map(t => (
        t._1, // person Id
        RecommendationUtils.getMinHashSignature(t._2, minHasher)) // MinHash signature
      )
      .name("Map: calculate MinHash signature for person")

  /**
    * Transforms the event streams into tuples (person id, post id) and emits a unioned stream of these tuples
    *
    * @param comments The stream of comments
    * @param posts    The stream of posts
    * @param likes    The stream of likes
    * @return Unioned stream of (person id, post id) tuples
    */
  def unionEvents(comments: DataStream[CommentEvent],
                  posts: DataStream[PostEvent],
                  likes: DataStream[LikeEvent]): DataStream[(Long, Long)] =
    comments
      .map(c => (c.personId, c.postId)).name("Map: -> (person Id, post Id)")
      .union(
        posts.map(p => (p.personId, p.postId)).name("Map: -> (person Id, post Id)"),
        likes.map(l => (l.personId, l.postId)).name("Map: -> (person Id, post Id)")
      )

  /**
    * Filters the set of candidates to those that have been active in a defined event time window.
    *
    * @param candidates      The stream of (person id, MinHash-signature, Set[candidate person ids])
    * @param forumEvents     The stream of all post interactions of persons, as (person id, post id) tuples
    * @param activityTimeout The timeout duration for person activities. If the last activity was outside of this time
    *                        window, the person is considered inactive.
    * @return Stream of (person id, person MinHash-signature, Set[candidate person id]) tuples, with inactive persons
    *         removed from the set of candidates.
    */
  def filterToActiveUsers(candidates: DataStream[(Long, MinHashSignature, Set[Long])],
                          forumEvents: DataStream[(Long, Long)],
                          activityTimeout: Time): DataStream[(Long, MinHashSignature, Set[Long])] = {
    // broadcast the stream of person ids for all forum events (with their implicit event timestamp) to all workers
    val broadcastActivePersons =
      forumEvents
        .map(_._1)
        .name("Map: -> person Id")
        .broadcast()

    // connect the candidates stream with the broadcast stream, and remove those candidates from the sets for which
    // no activity was recorded within the activity timeout before the event time of the candidate tuple (which
    // corresponds to the end time of the sliding window within which the person's activities were collected to
    // derive the activity-based MinHash signature)
    candidates
      .connect(broadcastActivePersons)
      .process(new FilterToActivePersonsFunction(activityTimeout))
      .name("CoProcess: filter to active persons")
  }

  /**
    * Collects the set of post ids that each person interacted with in a sliding event-time window.
    *
    * @param postInteractions The stream of all individual interactions of persons with posts
    *                         (creation, comments, likes)
    * @param windowSize       The size of the sliding window
    * @param windowSlide      The window slide length
    * @return
    */
  def collectPostsInteractedWith(postInteractions: DataStream[(Long, Long)],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, Set[Long])] =
    postInteractions
      .keyBy(_._1) // key by person id
      .timeWindow(size = FlinkUtils.convert(windowSize), slide = FlinkUtils.convert(windowSlide))
      .aggregate(
        new CollectSetFunction[(Long, Long), Long, Long](
          key = _._1, // function to extract person id
          value = _._2 // function to extract post id
        )
      )
      .name("Collect posts with which the person interacted " +
        s"(window: ${DateTimeUtils.formatDuration(windowSize.toMilliseconds)} " +
        s"slide ${DateTimeUtils.formatDuration(windowSlide.toMilliseconds)})")

  /**
    * For a stream of person ids with a MinHash signature representing the features for each person, persons with
    * similar feature sets are looked up based on their assignment to LSH buckets (stored in ElasticSearch by the
    * batch job for loading the static tables)
    *
    * @param personActivityMinHash The input stream of (person id, MinHash-signature) tuples
    * @param esNodes               The ElasticSearch nodes to connect to
    * @param minHasher             The MinHasher used to calculate the buckets to look up in the index
    * @return Stream of (person id, person MinHash-signature, Set[candidate person id]) tuples
    */
  def lookupCandidateUsers(personActivityMinHash: DataStream[(Long, MinHashSignature)])
                          (implicit esNodes: Seq[ElasticSearchNode],
                           minHasher: MinHasher32): DataStream[(Long, MinHashSignature, Set[Long])] = {
    FlinkUtils.asyncStream(
      personActivityMinHash,
      new AsyncCandidateUsersLookupFunction(
        ElasticSearchIndexes.lshBuckets.indexName,
        minHasher, esNodes: _*))
      .name("Async I/O: get candidates in same LSH buckets")
  }

  /**
    * For a stream of person ids with candidate persons, looks up persons already known to an input person, and
    * emits a stream of equally-shaped tuples exluding these known persons from the set of candidates.
    *
    * @param candidates The input stream of (person id, MinHash-signature, Set[candidate person id])
    * @param esNodes    The ElasticSearch nodes to connect to
    * @return Stream with known persons removed from the set of candidates
    */
  def excludeKnownPersons(candidates: DataStream[(Long, MinHashSignature, Set[Long])])
                         (implicit esNodes: Seq[ElasticSearchNode]): DataStream[(Long, MinHashSignature, Set[Long])] = {
    FlinkUtils.asyncStream(
      candidates,
      new AsyncExcludeKnownPersonsFunction(
        ElasticSearchIndexes.knownPersons.indexName,
        ElasticSearchIndexes.knownPersons.typeName,
        esNodes: _*))
      .name("Async I/O: exclude known persons from candidates")
  }

  /**
    * For the stream of person ids with their MinHash signatures and set of candidate persons to recommend, the
    * MinHash signatures stored for these candidate persons are looked up from an ElasticSearch index, and the
    * Jaccard similarity is approximated between the current person and each candidate, based on the MinHash signatures.
    * Based on the approximated similarity, the set of candidates is reduced to the top N exceeding a given minimum
    * similarity. An output stream is produced with tuples (person id, Seq[(recommended person id, similarity)]), with
    * the recommended persons ordered by decreasing similarity.
    *
    * @param candidates    The input stream of (person id, MinHash-signature, Set[candidate person id])
    * @param maxCount      The maximum number of persons to recommend
    * @param minSimilarity The minimum value for the approximated Jaccard similartiy (0 to 1) for a recommendation
    * @param esNodes       The ElasticSearch nodes to connect to
    * @param minHasher     The MinHasher, used to calculate the approximate similarity between two persons based on t
    *                      heir MinHash signatures.
    * @return Stream of recommendations per persion, as tuples (person id, Seq[(recommended person id, similarity)]),
    *         with the recommended persons ordered by decreasing similarity.
    */
  def recommendUsers(candidates: DataStream[(Long, MinHashSignature, Set[Long])],
                     maxCount: Int,
                     minSimilarity: Double)
                    (implicit esNodes: Seq[ElasticSearchNode],
                     minHasher: MinHasher32): DataStream[(Long, Seq[(Long, Double)])] =
    FlinkUtils.asyncStream(
      candidates,
      new AsyncRecommendUsersFunction(
        ElasticSearchIndexes.personMinHashes.indexName,
        minHasher, maxCount, minSimilarity,
        esNodes: _*))
      .name("Async I/O: sort and filter candidates based on their MinHash-approximated similarity")
}


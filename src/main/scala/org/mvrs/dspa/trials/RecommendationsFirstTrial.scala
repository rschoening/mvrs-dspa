package org.mvrs.dspa.trials

import java.nio.file.Paths
import java.util.Properties

import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.{IngestionTimeExtractor, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.time.Time
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.{CommentEvent, ForumEvent, LikeEvent, PostEvent}
import org.mvrs.dspa.{Settings, utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RecommendationsFirstTrial extends App {
  // set up the streaming execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val rootPath = Settings.tablesDirectory
  val hasInterestCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString

  val workStream = env
    .readTextFile(worksAtCsv)
    .filter(str => !str.startsWith("Person.id|") && str != "")
    .map(parseInterest(_, "W"))

  val tagsStream = env
    .readTextFile(hasInterestCsv)
    .filter(str => !str.startsWith("Person.id|") && str != "")
    .map(parseInterest(_, "T"))

  // union with dummy stream that does not end - otherwise the last trigger, after all sources close, is not processed!
  // workaround: https://stackoverflow.com/questions/52467413/processing-time-windows-doesnt-work-on-finite-data-sources-in-apache-flink
  val dummyInterest: String = ""
  val dummyStream: DataStream[(Long, String)] = env.addSource((context: SourceContext[(Long, String)]) => {
    while (true) {
      Thread.sleep(1000)
      context.collect((-1, dummyInterest))
    }
  })

  val minHasher: MinHasher32 = createMinHasher()

  // use ingestion time as event time, so we can apply time window with the time semantics we need later (when processing event streams)
  // note that times from this stream will not be compared with times from the event stream (resulting bucket state will be broadcast to event stream)
  val interestsStream: KeyedStream[(Long, String), Long] =
  tagsStream
    .union(
      tagsStream,
      dummyStream // the dummy stream to ensure trigger after static sources close
    ) // TODO combine with all other streams (study, tag classes etc.)
    .filter(_._2 != dummyInterest) // drop the dummy events
    .assignTimestampsAndWatermarks(new IngestionTimeExtractor[(Long, String)]())
    .keyBy(_._1) // (*uid*, interest)

  //  val interestsStream: KeyedStream[(Long, String), Long] =
  //    env.fromCollection(List((1L, "a"), (2L, "a"), (1L, "b"), (3L, "c"), (4L, "a")))
  //    .union(dummyStream)
  //    .filter(_._2 != dummyInterest)
  //    .assignTimestampsAndWatermarks(new IngestionTimeExtractor[(Long, String)]())
  //    .keyBy(_._1) // (*uid*, interest)

  //interestsStream.print

  // collect the users interests in a set
  val groupedInterestsStream: KeyedStream[(Long, Set[String]), Long] =
    interestsStream
      .process(new GroupInterestsProcessFunction(Time.seconds(5).toMilliseconds)) // (uid, Set[interest])
      .keyBy(_._1) // (*uid*, Set[interest])

  //groupedInterestsStream.print

  // based on the set of interests per user, calculate the minhash signature
  val userSignatureStream: KeyedStream[(Long, MinHashSignature), Long] =
    groupedInterestsStream
      .process(new HashGroupedInterestsFunction(minHasher)) // -> (uid, signature)
      .keyBy(_._1) // (*uid*, signature)

  //userSignatureStream.map(t => println(s"${t._1}: ${t._2.bytes.toList}"))

  // calculate the LSH bucket for each user/signature, key the stream by bucket
  val bucketedUserStream: KeyedStream[(Long, (Long, MinHashSignature)), Long] =
    userSignatureStream
      .map(new LSHBucketAssigner(minHasher)) // (uid, signature, List(bucket))
      .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, (t._1, t._2)))) // (bucket, (uid, signature))
      .keyBy(_._1) // (*bucket*, (uid, signature))

  //bucketedUserStream.map(t => println(s"${t._1}"))

  // group the users with their signatures per bucket
  val usersPerBucketStream: DataStream[(Long, Map[Long, MinHashSignature])] = bucketedUserStream
    .process(new GroupUsersByBucketFunction()) // (bucket, Map(uid, signature))
  //.keyBy(_._1) // (*bucket*, Map(uid, signature))

  // usersPerBucketStream.print

  // broadcast the buckets
  val broadcastStateDescriptor =
    new MapStateDescriptor[Long, Map[Long, MinHashSignature]](
      "buckets",
      createTypeInformation[Long],
      createTypeInformation[Map[Long, MinHashSignature]])

  val usersPerBucketBroadcastStream: BroadcastStream[(Long, Map[Long, MinHashSignature])] =
    usersPerBucketStream
      .broadcast(broadcastStateDescriptor)

  //get the events stream (posts, likes, comments)
  val events: KeyedStream[ForumEvent, Long] =
    Streams.events(env)
      .keyBy(_.personId) // (ForumEvent keyed by .personId)

  //val events = env.fromCollection[ForumEvent](List(LikeEvent(1, ZonedDateTime.now, 99))).keyBy(_.personId)

  // incomplete: get recommendations per user
  // TODO: get user interests based on session behavior (posts, likes, comments)
  val recommendations: DataStream[(Long, Set[(Long, Double)])] =
  events
    .connect(usersPerBucketBroadcastStream)
    .process(new RecommendUsersFunction(minHasher, broadcastStateDescriptor)) // (uid, Set[(uid, similarity)])

  // execute program
  env.execute("LSH of user interests based on static data")

  private def parseInterest(str: String, prefix: String): (Long, String) = {
    val tokens = str.split('|')
    (tokens(0).trim.toLong, prefix + tokens(1).trim)
  }

  private def createMinHasher(numHashes: Int = 100, targetThreshold: Double = 0.2): MinHasher32 =
    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))

  class RecommendUsersFunction(minHasher: MinHasher32, stateDescriptor: MapStateDescriptor[Long, Map[Long, MinHashSignature]])
    extends KeyedBroadcastProcessFunction[Long, ForumEvent, (Long, Map[Long, MinHashSignature]), (Long, Set[(Long, Double)])] {

    override def processElement(value: ForumEvent,
                                ctx: KeyedBroadcastProcessFunction[Long, ForumEvent, (Long, Map[Long, MinHashSignature]), (Long, Set[(Long, Double)])]#ReadOnlyContext,
                                out: Collector[(Long, Set[(Long, Double)])]): Unit = {
      val uid = value.personId

      // 1. get the interests for the user ---- where from?
      //    - time window --> liked posts, commented posts etc. --> get interests derived from these!
      //    - user properties (csv files) : OR: search in all buckets
      // 2. calculate minhash based on this user's interest
      // 3. calculate buckets
      // 4. get all users in same buckets, de-duplication
      // 5. order users by similarity, take top 5


      val state = ctx.getBroadcastState(stateDescriptor)

      // println(state.immutableEntries().asScala.count(_ => true))

      out.collect((uid, Set[(Long, Double)]()))
    }

    override def processBroadcastElement(value: (Long, Map[Long, MinHashSignature]),
                                         ctx: KeyedBroadcastProcessFunction[Long, ForumEvent, (Long, Map[Long, MinHashSignature]), (Long, Set[(Long, Double)])]#Context,
                                         out: Collector[(Long, Set[(Long, Double)])]): Unit = {
      // println(s"Received broadcast state: $value")
      val bucket = value._1
      val users = value._2
      val state = ctx.getBroadcastState(stateDescriptor)
      val count = state.entries().asScala.count(_ => true)
      state.put(bucket, users)
      val newcount = state.entries().asScala.count(_ => true)

      //println(s"bucket $bucket: $count --> $newcount ($users)")
    }
  }

  // NOTE alternative function for grouping interests into sets, in global windows with custom trigger
  // However this function always emits all sets when triggered, not only the changed sets. The ProcessFunction allows better control.
  // This is how the aggregate function would be used:
  //  val groupedInterestsStream =
  //    interestsStream
  //      .window(GlobalWindows.create())
  //      .trigger(ContinuousEventTimeTrigger.of(Time.milliseconds(10000)))
  //      .aggregate(new GroupInterestsAggregationFunction())
  //      .keyBy(_._1)
  class GroupInterestsAggregationFunction extends AggregateFunction[(Long, String), (Long, mutable.Set[String]), (Long, Set[String])] {
    override def createAccumulator(): (Long, mutable.Set[String]) = (0, mutable.Set[String]())

    override def add(value: (Long, String), accumulator: (Long, mutable.Set[String])): (Long, mutable.Set[String]) = {
      assert(accumulator._1 == 0 || value._1 == accumulator._1)
      (value._1, accumulator._2 + value._2)
    }

    // NOTE this always emits, even if the sets have not changed
    override def getResult(accumulator: (Long, mutable.Set[String])): (Long, Set[String]) = (accumulator._1, accumulator._2.toSet)

    override def merge(a: (Long, mutable.Set[String]), b: (Long, mutable.Set[String])): (Long, mutable.Set[String]) = {
      assert(a._1 == 0 || b._1 == 0 || a._1 == b._1)
      (b._1, a._2 ++ b._2)
    }
  }

  class GroupInterestsProcessFunction(val emitIntervalMillis: Long) extends KeyedProcessFunction[Long, (Long, String), (Long, Set[String])] {
    private lazy val interests: ValueState[mutable.Set[String]] =
      getRuntimeContext.getState(new ValueStateDescriptor("interests", classOf[mutable.Set[String]]))
    private lazy val timerSet: ValueState[Boolean] =
      getRuntimeContext.getState(new ValueStateDescriptor("timerSet", classOf[Boolean]))
    private lazy val changed: ValueState[Boolean] =
      getRuntimeContext.getState(new ValueStateDescriptor("changed", classOf[Boolean]))

    override def processElement(value: (Long, String),
                                ctx: KeyedProcessFunction[Long, (Long, String), (Long, Set[String])]#Context,
                                out: Collector[(Long, Set[String])]): Unit = {
      val set = getSet
      val origSize = set.size
      val interest = value._2

      set.add(interest) // add the interest to the set

      if (set.size != origSize) {
        // the set was changed, put it back into the state
        interests.update(set)

        if (!changed.value()) {
          changed.update(true)
        }
      }

      if (!timerSet.value()) {
        // note the overall time semantics is event time (required by the processing of the actual events), however here
        // we want a processing time window, since otherwise the last window would not be emitted
        setTimer(ctx)
        timerSet.update(true)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, (Long, String), (Long, Set[String])]#OnTimerContext,
                         out: Collector[(Long, Set[String])]): Unit = {
      // emit only if set is changed
      if (changed.value()) {
        out.collect((ctx.getCurrentKey, getSet.toSet))
        changed.update(false)
      }

      setTimer(ctx)
    }

    private def setTimer(ctx: KeyedProcessFunction[Long, (Long, String), (Long, Set[String])]#Context): Unit = {
      val timerService = ctx.timerService()
      timerService.registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + emitIntervalMillis)
    }

    private def getSet = if (interests == null || interests.value() == null) mutable.Set[String]() else interests.value
  }

  class LSHBucketAssigner(minHasher: MinHasher32)
    extends RichMapFunction[(Long, MinHashSignature), (Long, MinHashSignature, List[Long])] {
    override def map(value: (Long, MinHashSignature)): (Long, MinHashSignature, List[Long]) =
      (value._1, value._2, minHasher.buckets(value._2))
  }

  class GroupUsersByBucketFunction()
    extends KeyedProcessFunction[Long, (Long, (Long, MinHashSignature)), (Long, Map[Long, MinHashSignature])] {

    private lazy val bucketState: MapState[Long, MinHashSignature] =
      getRuntimeContext.getMapState(
        new MapStateDescriptor("bucket",
          createTypeInformation[Long],
          createTypeInformation[MinHashSignature]))

    override def processElement(value: (Long, (Long, MinHashSignature)),
                                ctx: KeyedProcessFunction[Long, (Long, (Long, MinHashSignature)), (Long, Map[Long, MinHashSignature])]#Context,
                                out: Collector[(Long, Map[Long, MinHashSignature])]): Unit = {
      val bucket = value._1
      val uid = value._2._1
      val signature = value._2._2

      // TODO this only ADDS to buckets, cannot remove user from bucket

      if (bucketState.contains(uid)) {
        if (!java.util.Arrays.equals(bucketState.get(uid).bytes, signature.bytes)) {
          // user is not yet in bucket
          bucketState.put(uid, signature)
          out.collect((bucket, toMap(bucketState)))
        }
      }
      else {
        // THIS IS PROBABLY WRONG, NEED TO UNDERSTAND BROADCAST STREAMS BETTER
        // - WHEN TO EMIT RESULT? HOW ARE THEY COLLECTED IN THE STATE OF THE RECEIVING STREAM?
        // - use global window with trigger, or timer defined here?
        bucketState.put(uid, signature)
        out.collect((bucket, toMap(bucketState)))
      }
    }

    /**
      * Converts from Flink MapState to immutable Map
      */
    private def toMap[K, V](mapState: MapState[K, V]): Map[K, V] =
      mapState.entries().asScala.map(e => (e.getKey, e.getValue)).toMap
  }

  class HashGroupedInterestsFunction(minHasher: MinHasher32)
    extends KeyedProcessFunction[Long, (Long, Set[String]), (Long, MinHashSignature)] {

    private lazy val minHashState: ValueState[MinHashSignature] =
      getRuntimeContext.getState[MinHashSignature](
        new ValueStateDescriptor[MinHashSignature]("signature", classOf[MinHashSignature]))

    override def processElement(value: (Long, Set[String]),
                                ctx: KeyedProcessFunction[Long, (Long, Set[String]), (Long, MinHashSignature)]#Context,
                                out: Collector[(Long, MinHashSignature)]): Unit = {
      val uid = value._1
      val interests = value._2

      val signature: MinHashSignature = minHasher.combineAll(interests.map(minHasher.init))

      minHashState.update(signature)

      out.collect((uid, signature))
    }
  }

}

object Streams {
  def events(implicit env: StreamExecutionEnvironment): DataStream[ForumEvent] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "test")
    props.setProperty("isolation.level", "read_committed")

    val commentsSource = utils.createKafkaConsumer("comments", createTypeInformation[CommentEvent], props)
    val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent], props)
    val likesSource = utils.createKafkaConsumer("likes", createTypeInformation[LikeEvent], props)

    val maxOutOfOrderness = Time.milliseconds(1000)

    val commentsStream = env
      .addSource(commentsSource) // TODO speedup/out-of-ordering
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timestamp))
      .keyBy(_.postId)

    val postsStream = env
      .addSource(postsSource) // TODO speedup/out-of-ordering
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timestamp))
      .keyBy(_.postId)

    val likesStream = env
      .addSource(likesSource) // TODO speedup/out-of-ordering
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timestamp))
      .keyBy(_.postId)

    commentsStream
      .map(_.asInstanceOf[ForumEvent])
      .union(
        postsStream.map(_.asInstanceOf[ForumEvent]),
        likesStream.map(_.asInstanceOf[ForumEvent]))
  }
}

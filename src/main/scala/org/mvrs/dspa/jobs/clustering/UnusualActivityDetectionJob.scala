package org.mvrs.dspa.jobs.clustering

import java.util.concurrent.TimeUnit

import com.google.common.base.Splitter
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.{Settings, streams}

import scala.collection.JavaConverters._
import scala.collection.mutable

object UnusualActivityDetectionJob extends FlinkStreamingJob {
  // TODO
  // - add integration tests, refactor for testability
  // - extract features within clustering operator (more flexibility to standardize/normalize features)
  // - if a cluster gets too small, split the largest cluster
  // - come up with better text features
  // - show raw clusters in kibana (saved search)

  val controlFilePath = Settings.config.getString("jobs.activity-detection.control-stream-path")
  val frequencyWindowSize = Time.hours(12)
  val frequencyWindowSlide = Time.hours(1)
  val defaultK = 4
  val defaultDecay = 0.2
  val clusterWindowSize = Time.hours(24)
  val minClusterElementCount = 100
  val maxClusterElementCount = 20000
  val aggregateFeaturesStateTtl = FlinkUtils.getTtl(Time.of(3, TimeUnit.HOURS), Settings.config.getInt("data.speedup-factor"))

  ElasticSearchIndexes.classification.create()
  ElasticSearchIndexes.clusterMetadata.create()

  // val kafkaConsumerGroup = Some("activity-detection")
  val comments: DataStream[CommentEvent] = streams.comments()
  val posts: DataStream[PostEvent] = streams.posts()

  // read raw control file lines
  val controlParameterLines: DataStream[String] = readControlParameters(controlFilePath)

  // parse into valid parameters and parse error streams
  val (controlParameters, controlParameterParseErrors) = parseControlParameters(controlParameterLines)

  // get featurized comments and posts in a unioned stream
  val eventFeaturesStream: DataStream[FeaturizedEvent] = getEventFeatures(comments, posts)

  // get frequency of posts/comments in time window, per person
  val frequencyStream: DataStream[(Long, Int)] =
    getEventFrequencyPerPerson(eventFeaturesStream, frequencyWindowSize, frequencyWindowSlide)

  // get aggregated features (text and frequency-based)
  val aggregatedFeaturesStream: DataStream[FeaturizedEvent] =
    aggregateFeatures(eventFeaturesStream, frequencyStream, aggregateFeaturesStateTtl)

  // cluster combined features (on a single worker) in a custom window:
  // - tumbling window of configured size
  // - ... but never exceeding maximum event count (early firing)
  // - ... and making sure that there is a minimum number of events (extending the window if needed)

  // if to be parallelized: distribute points randomly, cluster subsets, merge resulting clusters as in
  // 7.6.4 of "Mining of massive datasets"

  val (clusterModelStream: DataStream[(Long, Int, ClusterModel)], clusterMetadata: DataStream[ClusterMetadata]) =
    updateClusterModel(
      aggregatedFeaturesStream, controlParameters,
      defaultK, defaultDecay,
      clusterWindowSize, minClusterElementCount, maxClusterElementCount)

  // classify (in parallel) events with aggregated features based on broadcasted cluster model
  val classifiedEvents: DataStream[ClassifiedEvent] = classifyEvents(aggregatedFeaturesStream, clusterModelStream)

  // set up sinks

  // write classification result to kafka/elasticsearch
  classifiedEvents
    .addSink(ElasticSearchIndexes.classification.createSink(1))
    .name(s"elastic search: ${ElasticSearchIndexes.classification.indexName}")

  clusterMetadata
    .addSink(ElasticSearchIndexes.clusterMetadata.createSink(5))
    .name(s"elastic search: ${ElasticSearchIndexes.clusterMetadata.indexName}")

  controlParameterParseErrors.print("Parameter parse error") // TODO write to rolling log file

  env.execute()

  def readControlParameters(controlFilePath: String, updateInterval: Long = 2000L)
                           (implicit env: StreamExecutionEnvironment): DataStream[String] =
    env
      .readFile(
        new TextInputFormat(new Path(controlFilePath)),
        controlFilePath,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        updateInterval).name("Read clustering parameters")
      .name("control stream source")
      .setParallelism(1) // otherwise the empty splits never emit watermarks, timers never fire etc.
      .assignTimestampsAndWatermarks(FlinkUtils.timeStampExtractor[String](Time.seconds(0), _ => Long.MaxValue)) // required for downstream timers

  def parseControlParameters(controlParametersParsed: DataStream[String]): (DataStream[ClusteringParameter], DataStream[Throwable]) = {
    val controlParametersParsed: DataStream[Either[Throwable, ClusteringParameter]] =
      controlParameterLines
        .flatMap(ClusteringParameter.parse _).name("Parse parameters")
        .setParallelism(1)

    val controlParameterParseErrors: DataStream[Throwable] =
      controlParametersParsed
        .filter(_.isLeft).setParallelism(1)
        .map(_.left.get).setParallelism(1)
        .name("Parameter parse errors")

    val controlParameters: DataStream[ClusteringParameter] =
      controlParametersParsed
        .filter(_.isRight).setParallelism(1)
        .map(_.right.get).setParallelism(1)
        .name("Control stream for clustering parameters")

    (controlParameters, controlParameterParseErrors)
  }

  def getEventFeatures(comments: DataStream[CommentEvent],
                       posts: DataStream[PostEvent]): DataStream[FeaturizedEvent] = {
    val commentFeaturesStream =
      comments
        .map(
          c => FeaturizedEvent(
            c.personId,
            if (c.isReply) EventType.Reply else EventType.Comment,
            c.commentId,
            c.timestamp,
            extractTextFeatures(c.content))
        )
        .name("extract comment features")

    val postFeaturesStream =
      posts
        .map(
          p => FeaturizedEvent(
            p.personId,
            EventType.Post,
            p.postId,
            p.timestamp,
            extractTextFeatures(p.content))
        )
        .name("extract post features")

    // return unioned stream
    commentFeaturesStream.union(postFeaturesStream)
  }

  def getEventFrequencyPerPerson(eventFeaturesStream: DataStream[FeaturizedEvent],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, Int)] =
    eventFeaturesStream
      .map(c => (c.personId, 1))
      .keyBy(_._1)
      .timeWindow(FlinkUtils.convert(windowSize), FlinkUtils.convert(windowSlide))
      .sum(1)
      .name("calculate post/comment frequency per person")

  def aggregateFeatures(eventFeaturesStream: DataStream[FeaturizedEvent],
                        frequencyStream: DataStream[(Long, Int)],
                        stateTtl: Time): DataStream[FeaturizedEvent] =
    eventFeaturesStream
      .keyBy(_.personId)
      .connect(frequencyStream.keyBy(_._1)) // both streams keyed on person id
      .process(new AggregateFeaturesFunction(stateTtl)) // join event with latest known frequency for the person
      .name("aggregate features")

  def updateClusterModel(aggregatedFeaturesStream: DataStream[FeaturizedEvent],
                         controlParameters: DataStream[ClusteringParameter],
                         k: Int, decay: Double, windowSize: Time,
                         minElementCount: Int, maxElementCount: Int): (DataStream[(Long, Int, ClusterModel)], DataStream[ClusterMetadata]) = {
    val clusterParametersBroadcastStateDescriptor =
      new MapStateDescriptor[String, ClusteringParameter](
        "cluster-parameters",
        classOf[String],
        classOf[ClusteringParameter])

    val controlParameterBroadcast: BroadcastStream[ClusteringParameter] =
      controlParameters.broadcast(clusterParametersBroadcastStateDescriptor)

    val outputTagClusterMetadata = new OutputTag[ClusterMetadata]("cluster-metadata")

    val clusters: DataStream[(Long, Int, ClusterModel)] =
      aggregatedFeaturesStream
        .map(_.features) // feature vector
        .keyBy(_ => 0) // all to same worker
        .connect(controlParameterBroadcast)
        .process(
          new KMeansClusterFunction(
            k, decay, windowSize, minElementCount, maxElementCount,
            clusterParametersBroadcastStateDescriptor,
            Some(outputTagClusterMetadata))).name("calculate clusters")
        .setParallelism(1)

    val clusterMetadata: DataStream[ClusterMetadata] = clusters.getSideOutput(outputTagClusterMetadata)

    (clusters, clusterMetadata)
  }

  def classifyEvents(aggregatedFeaturesStream: DataStream[FeaturizedEvent],
                     clusterModelStream: DataStream[(Long, Int, ClusterModel)]): DataStream[ClassifiedEvent] = {
    // broadcast stream for clusters
    val clusterStateDescriptor =
      new MapStateDescriptor(
        "cluster-broadcast-state",
        createTypeInformation[Int],
        createTypeInformation[(Long, Int, ClusterModel)])

    // broadcast the cluster state
    val broadcast = clusterModelStream.broadcast(clusterStateDescriptor)

    // connect feature stream with cluster broadcast, classify featurized comments
    aggregatedFeaturesStream
      .keyBy(_.personId)
      .connect(broadcast)
      .process(new ClassifyEventsFunction(clusterStateDescriptor)).name("classify comments")
  }

  def extractTextFeatures(content: Option[String]): mutable.ArrayBuffer[Double] = {
    val tokens =
      content
        .map(tokenize(_).toVector)
        .getOrElse(Vector.empty[String])

    val dim = 3
    val buffer = new mutable.ArrayBuffer[Double](dim)

    if (tokens.isEmpty) buffer ++= List.fill(dim)(0.0) // zero vector
    else {
      // TODO how to scale/normalize features?
      // TODO do this in windowing function to allow normalization/standardization?

      // buffer += math.log(tokens.size)
      // buffer += math.log(tokens.map(_.toLowerCase()).distinct.size) // distinct word count
      buffer += 10 * tokens.map(_.toLowerCase()).distinct.size / tokens.size // proportion of distinct words
      // ... and some bogus features:
      buffer += tokens.count(_.forall(_.isUpper)) / tokens.size // % of all-UPPERCASE words
      buffer += tokens.count(_.length == 4) / tokens.size // % of four-letter words
    }
  }

  /**
    * guava's splitter to tokenize content
    */
  private lazy val splitter =
    Splitter
      .onPattern("[\\s,.;]+")
      .omitEmptyStrings()
      .trimResults()

  private def tokenize(str: String): Iterable[String] = splitter.split(str).asScala

}




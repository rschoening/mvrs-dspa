package org.mvrs.dspa.jobs.clustering

import com.google.common.base.Splitter
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkStreamingJob
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.elastic.ElasticSearchNode
import org.mvrs.dspa.utils.{DateTimeUtils, FlinkUtils}
import org.mvrs.dspa.{Settings, streams}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/**
  * Streaming job for unusual activity detection (DSPA Task #3)
  *
  * @todo add integration tests
  *       come up with better text features
  *       if a cluster gets too small, split the largest cluster
  *       extract features within clustering operator (more flexibility to standardize/normalize features)
  */
object UnusualActivityDetectionJob extends FlinkStreamingJob(enableGenericTypes = true) {

  def execute(): JobExecutionResult = {
    // read settings
    val clusterParameterFilePath = Settings.config.getString("jobs.activity-detection.cluster-parameter-file-path")
    val clusterParameterParseErrorsOutputPath = Settings.config.getString("jobs.activity-detection.cluster-parameter-file-parse-errors-path")
    val frequencyWindowSize = Settings.duration("jobs.activity-detection.frequency-window-size")
    val frequencyWindowSlide = Settings.duration("jobs.activity-detection.frequency-window-slide")
    val ignoreActivityOlderThan = Settings.duration("jobs.activity-detection.ignore-activity-older-than")
    val defaultK = Settings.config.getInt("jobs.activity-detection.default-k")
    val defaultDecay = Settings.config.getDouble("jobs.activity-detection.default-decay")
    val clusterWindowSize = Settings.duration("jobs.activity-detection.cluster-window-size")
    val minClusterElementCount = Settings.config.getInt("jobs.activity-detection.minimum-cluster-element-count")
    val maxClusterElementCount = Settings.config.getInt("jobs.activity-detection.maximum-cluster-element.count")
    val speedupFactor = Settings.config.getInt("data.speedup-factor")
    val classifiedEventsBatchSize = Settings.config.getInt("jobs.activity-detection.classified-events-elasticsearch-batch-size")
    val clusterMetadataBatchSize = Settings.config.getInt("jobs.activity-detection.cluster-metadata-elasticsearch-batch-size")

    // implicits
    implicit val esNodes: Seq[ElasticSearchNode] = Settings.elasticSearchNodes

    // (re)create ElasticSearch indexes for classification results and cluster metadata
    ElasticSearchIndexes.classification.create()
    ElasticSearchIndexes.clusterMetadata.create()

    // consume comments and posts from Kafka
    val kafkaConsumerGroup = Some("activity-detection")
    val comments: DataStream[CommentEvent] = streams.comments(kafkaConsumerGroup)
    val posts: DataStream[PostEvent] = streams.posts(kafkaConsumerGroup)

    // read raw control file lines
    val controlParameterLines: DataStream[String] = readControlParameters(clusterParameterFilePath)

    // parse into valid parameters and parse error streams
    val (controlParameters: DataStream[ClusteringParameter], controlParameterParseErrors: DataStream[String]) =
      parseControlParameters(controlParameterLines)

    // get featurized comments and posts in a unioned stream
    val eventFeaturesStream: DataStream[FeaturizedEvent] = getEventFeatures(comments, posts)

    // get frequency of posts/comments in time window, per person
    val frequencyStream: DataStream[(Long, Int)] = getEventFrequencyPerPerson(
      comments, posts, frequencyWindowSize, frequencyWindowSlide)

    // get aggregated features (text and frequency-based)
    val aggregatedFeaturesStream: DataStream[FeaturizedEvent] =
      aggregateFeatures(
        eventFeaturesStream, frequencyStream,
        ignoreActivityOlderThan, FlinkUtils.getTtl(frequencyWindowSize, speedupFactor))

    // cluster combined features (on a single worker, parallelizable if needed) in a custom window:
    // - tumbling window of configured size
    // - ... but never exceeding maximum event count (early firing)
    // - ... and making sure that there is a minimum number of events (extending the window if needed)
    val (clusterModelStream: DataStream[(Long, Int, ClusterModel)], clusterMetadata: DataStream[ClusterMetadata]) =
    updateClusterModel(
      aggregatedFeaturesStream, controlParameters,
      defaultK, defaultDecay,
      clusterWindowSize, minClusterElementCount, maxClusterElementCount)

    // classify (in parallel) events with aggregated features based on broadcasted cluster model
    val classifiedEvents: DataStream[ClassifiedEvent] = classifyEvents(aggregatedFeaturesStream, clusterModelStream)

    // set up sinks

    // write classification result to ElasticSearch
    classifiedEvents
      .addSink(ElasticSearchIndexes.classification.createSink(classifiedEventsBatchSize))
      .name(s"ElasticSearch: ${ElasticSearchIndexes.classification.indexName}")

    // write cluster metadata to ElasticSearch
    clusterMetadata
      .addSink(ElasticSearchIndexes.clusterMetadata.createSink(clusterMetadataBatchSize))
      .name(s"ElasticSearch: ${ElasticSearchIndexes.clusterMetadata.indexName}")

    // write cluster parameter parse errors to text file sink
    outputErrors(controlParameterParseErrors, clusterParameterParseErrorsOutputPath)

    // print progress information for any late classification results
    FlinkUtils.addProgressMonitor(classifiedEvents, prefix = "CLEV") { case (_, progressInfo) => progressInfo.isLate }
    // ... and cluster metadata
    FlinkUtils.addProgressMonitor(clusterMetadata, prefix = "META") { case (_, progressInfo) => progressInfo.isLate }

    FlinkUtils.printExecutionPlan()

    env.execute("Classify events")
  }

  /**
    * Continuously read updated clustering control parameter lines from a text file
    *
    * @param controlFilePath The path to the control parameters file
    * @param updateInterval  The time interval (in millis) between consecutive path scans
    * @param env             The stream execution environment
    * @return Stream of control parameter file lines (yet unparsed)
    */
  def readControlParameters(controlFilePath: String, updateInterval: Long = 1000L)
                           (implicit env: StreamExecutionEnvironment): DataStream[String] = {
    val inputFormat = new TextInputFormat(new Path(controlFilePath))
    inputFormat.setNumSplits(1)

    // TODO after recovery, no watermark is emitted, causing downstream operators to stall.
    // This affects the cluster model update operator. Restarts only after a fake edit in the file...
    // This is probably a general problem for such a control stream -> research possible solutions

    env
      .readFile(inputFormat, controlFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, updateInterval)
      .name(s"Control parameters: $controlFilePath")
      .setParallelism(1) // otherwise the empty splits never emit watermarks, timers never fire etc.
      .assignTimestampsAndWatermarks(
      FlinkUtils.timeStampExtractor[String](
        Time.seconds(0),
        _ => Long.MaxValue)) // fake timestamp at MaxValue; required for downstream timers
  }

  /**
    * Parses the stream control parameter lines and emits a stream of valid parsed parameters and a stream of error
    * messages.
    *
    * @param controlParameterLines The input stream of control parameter lines
    * @return Pair of streams: parameter stream and error message stream
    */
  def parseControlParameters(controlParameterLines: DataStream[String]): (DataStream[ClusteringParameter], DataStream[String]) = {
    val controlParametersParsed: DataStream[Either[String, ClusteringParameter]] =
      controlParameterLines
        .flatMap(ClusteringParameter.parse(_).map(_.left.map(_.getMessage))) // map the left side (parse exception) to a message string
        .setParallelism(1)
        .name("FlatMap: Parse parameters")

    // filtered stream of only parse errors
    val controlParameterParseErrors: DataStream[String] =
      controlParametersParsed
        .filter(_.isLeft)
        .name("Filter: parse errors").setParallelism(1)
        .map(_.left.get).setParallelism(1)
        .name("Map: Parameter parse errors")

    // filtered stream of only valid parameters
    val controlParameters: DataStream[ClusteringParameter] =
      controlParametersParsed
        .filter(_.isRight)
        .name("Filter: valid cluster parameters").setParallelism(1)
        .map(_.right.get).setParallelism(1)
        .name("Map: Control stream for clustering parameters")

    (controlParameters, controlParameterParseErrors)
  }

  /**
    * Outputs parse errors either to a text file sink, or to the standard output stream
    *
    * @param errors     Input stream of error messages
    * @param outputPath Path to the text file, or null if the standard output should be used
    */
  def outputErrors(errors: DataStream[String], outputPath: String): Unit =
    if (outputPath != null && !outputPath.trim.isEmpty)
      errors
        .addSink(createParseErrorSink(outputPath)).setParallelism(1).name(
        s"Control parameter parse errors: $outputPath")
    else
      errors
        .print()
        .name("Print control parameter parse errors")

  /**
    * Create the text file sink for parse errors
    *
    * @param outputPath The path to the text file output
    * @return File sink
    */
  def createParseErrorSink(outputPath: String): StreamingFileSink[String] = {
    StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new BasePathBucketAssigner[String]())
      .build()
  }

  /**
    * Extracts text-based features from input streams of comments and posts, and returns a unioned stream of
    * featurized events (carrying the feature vectors plus core information about the events)
    *
    * @param comments The input stream of comment events
    * @param posts    The input stream of post events
    * @return The stream of featurized events
    */
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
        .name("Map: Extract comment features")

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
        .name("Map: Extract post features")

    // return unioned stream
    commentFeaturesStream.union(postFeaturesStream)
  }

  /**
    * Produces a stream of post/comment counts per person, for a sliding window
    *
    * @param comments    The input stream of comments
    * @param posts       The input stream of posts
    * @param windowSize  The size of the sliding window
    * @param windowSlide The slide duration of the sliding window
    * @return Stream of pairs (person id, post/comment count within the window) emitted at the end of the sliding window
    */
  def getEventFrequencyPerPerson(comments: DataStream[CommentEvent],
                                 posts: DataStream[PostEvent],
                                 windowSize: Time,
                                 windowSlide: Time): DataStream[(Long, Int)] = {
    val frequencyInputStream =
      comments
        .map(c => (c.personId, 1))
        .name("Map: comment -> (person Id, counter)")
        .union(posts
          .map(p => (p.personId, 1))
          .name("Map: post -> (person Id, counter)"))
        .keyBy(_._1) // person id

    frequencyInputStream
      .timeWindow(FlinkUtils.convert(windowSize), FlinkUtils.convert(windowSlide))
      .sum(1) // 0-based, 1 -> counter
      .name("Calculate post/comment frequency per person" +
      s"(window: ${DateTimeUtils.formatDuration(windowSize.toMilliseconds)} " +
      s"slide ${DateTimeUtils.formatDuration(windowSlide.toMilliseconds)})")
  }

  /**
    * Extend the feature vector of featurized events based on activity frequency information by person,
    * from a second stream
    *
    * @param eventFeaturesStream     The input stream of featurized events
    * @param frequencyStream         The stream to connect with, containing activity frequency information by person
    * @param ignoreActivityOlderThan A duration in event time to identify frequency results that are too old to be
    *                                relevant.
    * @param stateTtl                The time-to-live for the keyed state representing the latest known event/post
    *                                frequency of the person.
    * @return Stream of events with extended feature vector
    */
  def aggregateFeatures(eventFeaturesStream: DataStream[FeaturizedEvent],
                        frequencyStream: DataStream[(Long, Int)],
                        ignoreActivityOlderThan: Time,
                        stateTtl: Time): DataStream[FeaturizedEvent] =
    eventFeaturesStream
      .keyBy(_.personId)
      .connect(frequencyStream.keyBy(_._1)) // both streams keyed on person id
      .process(new AggregateFeaturesFunction(ignoreActivityOlderThan, stateTtl)) // join event with latest known frequency for the person
      .name("CoProcess: Aggregate features")

  /**
    * Updates a cluster model by collecting feature vectors for events in a tumbling window (which may be extended or
    * fire early, based on minimum/maximum vectors for clustering), applying K-Means to the feature vectors at the end
    * of the window (using the previous model as initial centroids), and doing a weighted combination of the new
    * cluster model with the previous model (applying a decay factor to reduce the weight of the previous model)
    *
    * @param aggregatedFeaturesStream The stream of featurized events (posts and comments, features derived from text
    *                                 content, and the frequency of previous activity of the author of the event)
    * @param controlParameters        A stream of control parameters for parameterizing the clustering (k, decay) and
    *                                 to label the resulting clusters for easier interpretation
    * @param k                        initial value for number of clusters (can be altered via control stream)
    * @param decay                    initial value for decay factor, applied to cluster weights of preceding cluster
    *                                 model; a value of 1.0 results in equal per-point weight of old and new cluster;
    *                                 a value of 0.0 ignores the old cluster weight and sets the output weight to the
    *                                 number of points from the current window that were assigned to the cluster.
    * @param windowSize               the event time size of the tumbling window at the end of which the cluster model
    *                                 is updated
    * @param minElementCount          the minimum element count for updating the cluster model. If not enough elements
    *                                 arrived within the window size, the window is extended until the minimum value
    *                                 is reached.
    * @param maxElementCount          the maximum element count for an update of the cluster model. If this number of
    *                                 elements is reached, the cluster model is updated and emitted, and a new regular
    *                                 window is initiated (count-based early firing)
    * @return Pair of streams: updated cluster models as (timestamp, point count, new cluster model), and metadata on
    *         the cluster model update (weight difference, centroid movement, k difference)
    * @note         The clustering is currently done on a single worker. This could be parallelized if needed:
    *               1. distribute points randomly to multiple workers
    *               2. cluster subsets independently
    *               3. merge resulting clusters as outlined in 7.6.4 of "Mining of massive datasets"
    */
  def updateClusterModel(aggregatedFeaturesStream: DataStream[FeaturizedEvent],
                         controlParameters: DataStream[ClusteringParameter],
                         k: Int, decay: Double, windowSize: Time,
                         minElementCount: Int,
                         maxElementCount: Int): (DataStream[(Long, Int, ClusterModel)], DataStream[ClusterMetadata]) = {
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
        .map(_.features.toVector) // feature vector
        .name("Map -> feature vector")
        .keyBy(_ => 0) // all to same worker
        .connect(controlParameterBroadcast)
        .process(
          new KMeansClusterFunction(
            k, decay, windowSize, minElementCount, maxElementCount,
            clusterParametersBroadcastStateDescriptor,
            Some(outputTagClusterMetadata),
            random = new Random(137)) // use fixed seed to get deterministic seed points for clusters
        ).name("KeyedBroadcastProcess: update cluster model")
        .setParallelism(1)

    val clusterMetadata: DataStream[ClusterMetadata] = clusters.getSideOutput(outputTagClusterMetadata)

    (clusters, clusterMetadata)
  }

  /**
    * Broadcasts the updated cluster models to multiple workers that consume the stream of featurized events and
    * classify them by mapping them to the closest cluster centroid, producing an output stream of classified events
    *
    * @param aggregatedFeaturesStream The input stream of featurized events
    * @param clusterModelStream       The input stream of new cluster models
    * @return Stream of classified events
    */
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
      .process(new ClassifyEventsFunction(clusterStateDescriptor))
      .name("KeyedBroadcastProcess: classify comments")
  }

  /**
    * Extracts a feature vector from text.
    *
    * @param content The text to extract the feature vector from
    * @return The feature vector (would in reality be very large)
    * @note This is just a placeholder for feature extraction based on natural language analysis and in general,
    *       real data science.
    *       A streaming-specific concern might be how to standardize/normalize these features (whatever they are),
    *       potentially based on distribution information for a sliding window
    */
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

      buffer += 10 * tokens.map(_.toLowerCase()).distinct.size / tokens.size // proportion of distinct words
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
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
    // NOTE generic types have to be enabled, since reading the control parameter file using TextInputFormat
    // causes "type org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit is treated as a generic type"
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
        eventFeaturesStream,
        frequencyStream,
        FlinkUtils.getTtl(Time.hours(3), speedupFactor)
      )

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

    FlinkUtils.printExecutionPlan()

    env.execute()
  }

  /**
    * Continuously read updated clustering control parameters from a text file
    *
    * @param controlFilePath The path to the control parameters file
    * @param updateInterval
    * @param env
    * @return
    */
  def readControlParameters(controlFilePath: String, updateInterval: Long = 1000L)
                           (implicit env: StreamExecutionEnvironment): DataStream[String] = {
    val inputFormat = new TextInputFormat(new Path(controlFilePath))
    inputFormat.setNumSplits(1)

    env
      .readFile(inputFormat, controlFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, updateInterval)
      .name(s"Control parameters: $controlFilePath")
      .setParallelism(1) // otherwise the empty splits never emit watermarks, timers never fire etc.
      .assignTimestampsAndWatermarks(
      FlinkUtils.timeStampExtractor[String](
        Time.seconds(0),
        _ => Long.MaxValue)) // required for downstream timers
  }

  def parseControlParameters(controlParameterLines: DataStream[String]): (DataStream[ClusteringParameter], DataStream[String]) = {
    val controlParametersParsed: DataStream[Either[String, ClusteringParameter]] =
      controlParameterLines
        .flatMap(ClusteringParameter.parse(_).map(_.left.map(_.getMessage))) // left: parse error message
        .setParallelism(1)
        .name("Parse parameters")

    val controlParameterParseErrors: DataStream[String] =
      controlParametersParsed
        .filter(_.isLeft).name("Filter: parse errors").setParallelism(1)
        .map(_.left.get).setParallelism(1)
        .name("Parameter parse errors")

    val controlParameters: DataStream[ClusteringParameter] =
      controlParametersParsed
        .filter(_.isRight).name("Filter: valid cluster parameters").setParallelism(1)
        .map(_.right.get).setParallelism(1)
        .name("Control stream for clustering parameters")

    (controlParameters, controlParameterParseErrors)
  }

  def outputErrors(errors: DataStream[String], outputPath: String): Unit =
    if (outputPath != null && !outputPath.trim.isEmpty)
      errors
        .addSink(createParseErrorSink(outputPath)).setParallelism(1).name(
        s"Control parameter parse errors: $outputPath")
    else
      errors
        .print()
        .name("Print control parameter parse errors")

  def createParseErrorSink(outputPath: String): StreamingFileSink[String] = {
    StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new BasePathBucketAssigner[String]())
      .build()
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
      .sum(1)
      .name("calculate post/comment frequency per person" +
        s"(window: ${DateTimeUtils.formatDuration(windowSize.toMilliseconds)} " +
        s"slide ${DateTimeUtils.formatDuration(windowSlide.toMilliseconds)})")
  }

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
        .map(_.features.toVector) // feature vector
        .name("Map: --> feature vector")
        .keyBy(_ => 0) // all to same worker
        .connect(controlParameterBroadcast)
        .process(
          new KMeansClusterFunction(
            k, decay, windowSize, minElementCount, maxElementCount,
            clusterParametersBroadcastStateDescriptor,
            Some(outputTagClusterMetadata)))
        .name("Update cluster model")
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
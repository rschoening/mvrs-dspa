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
import org.mvrs.dspa.model.{ClassifiedEvent, ClusterMetadata, ClusterModel, EventType}
import org.mvrs.dspa.utils.FlinkStreamingJob
import org.mvrs.dspa.{Settings, streams, utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

object UnusualActivityDetectionJob extends FlinkStreamingJob {
  // TODO
  // - add integration tests, refactor for testability
  // - extract features within clustering operator (more flexibility to standardize/normalize features)
  // - if a cluster gets too small, split the largest cluster
  // - come up with better text features

  val controlFilePath = Settings.config.getString("jobs.activity-detection.control-stream-path")

  ElasticSearchIndexes.classification.create()
  ElasticSearchIndexes.clusterMetadata.create()

  // set up clustering stream:
  // - union of rooted comments and posts
  // - extract features from each event --> (person-id, Vector[Double])
  // - gather events on a single worker, in a tumbling window with trigger at maximum
  // - do k-means at end of window (merging with previous cluster centers)
  // - broadcast resulting clusters
  // - NOTE: previous clusters are used as seed points for new clusters --> new clusters mapped to old clusters simply by cluster index
  // - side output: cluster center difference

  val clusterParametersBroadcastStateDescriptor =
    new MapStateDescriptor[String, ClusteringParameter](
      "cluster-parameters",
      classOf[String],
      classOf[ClusteringParameter])

  val controlParameters: DataStream[Either[Throwable, ClusteringParameter]] =
    env
      .readFile(
        new TextInputFormat(new Path(controlFilePath)),
        controlFilePath,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        interval = 2000L).name("Read clustering parameters")
      .name("control stream source")
      .setParallelism(1) // otherwise the empty splits never emit watermarks, timers never fire etc.
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[String](Time.seconds(0), _ => Long.MaxValue)) // required for downstream timers
      .flatMap(ClusteringParameter.parse _).name("Parse parameters")
      .setParallelism(1)

  val controlParameterBroadcast: BroadcastStream[ClusteringParameter] =
    controlParameters
      .filter(_.isRight).setParallelism(1)
      .map(_.right.get).setParallelism(1)
      .name("Control stream for clustering parameters")
      .broadcast(clusterParametersBroadcastStateDescriptor)


  val controlParameterParseErrors =
    controlParameters
      .filter(_.isLeft).setParallelism(1)
      .map(_.left.get).setParallelism(1)
      .name("Parameter parse errors")
      .print("Parameter parse error").setParallelism(1) // TODO write to rolling log file

  // val comments = streams.commentsFromKafka("activity-detection")
  val comments = streams.comments()
  val posts = streams.posts()

  val commentFeaturesStream =
    comments
      .map(c => FeaturizedEvent(c.personId, if (c.isReply) EventType.Reply else EventType.Comment, c.commentId, extractTextFeatures(c.content)))
      .name("extract comment features")

  val postFeaturesStream =
    posts
      .map(c => FeaturizedEvent(c.personId, EventType.Post, c.postId, extractTextFeatures(c.content)))
      .name("extract post features")

  val eventFeaturesStream =
    commentFeaturesStream
      .union(postFeaturesStream)
      .keyBy(_.personId)

  val frequencyStream: DataStream[(Long, Int)] =
    eventFeaturesStream
      .map(c => (c.personId, 1))
      .keyBy(_._1)
      .timeWindow(utils.convert(Time.hours(12)), utils.convert(Time.hours(1)))
      .sum(1)
      .name("calculate comment frequency per user")
      .keyBy(_._1)


  val aggregatedFeaturesStream: DataStream[FeaturizedEvent] =
    eventFeaturesStream
      .connect(frequencyStream) // both keyed on person id
      .process(
      new AggregateFeaturesFunction(
        utils.getTtl(
          Time.of(3, TimeUnit.HOURS),
          Settings.config.getInt("data.speedup-factor"))
      )) // join event with latest known frequency for the person
      .name("aggregate features")

  // cluster combined features (on a single worker)
  // To parallelize: distribute points randomly, cluster subsets, merge resulting clusters as in
  // 7.6.4 of "Mining of massive datasets"

  // featurizedComments.process(new ProgressMonitorFunction[(Long, Long, ArrayBuffer[Double])]("FEATURIZED", 1))

  val outputTagClusterMetadata = new OutputTag[ClusterMetadata]("cluster metadata")

  val clusters: DataStream[(Long, Int, ClusterModel)] =
    aggregatedFeaturesStream
      .map(_.features) // feature vector
      .keyBy(_ => 0) // all to same worker
      .connect(controlParameterBroadcast)
      .process(
        new KMeansClusterFunction(
          k = 4, decay = 0.2,
          Time.hours(24), 100, 20000,
          clusterParametersBroadcastStateDescriptor,
          Some(outputTagClusterMetadata))).name("calculate clusters")
      .setParallelism(1)

  val clusterMetadata = clusters.getSideOutput(outputTagClusterMetadata)

  clusterMetadata
    .addSink(ElasticSearchIndexes.clusterMetadata.createSink(5))
    .name(s"elastic search: ${ElasticSearchIndexes.clusterMetadata.indexName}")

  // broadcast stream for clusters
  val clusterStateDescriptor =
    new MapStateDescriptor(
      "cluster-broadcast-state",
      createTypeInformation[Int],
      createTypeInformation[(Long, Int, ClusterModel)])

  // broadcast the cluster state
  val broadcast = clusters.broadcast(clusterStateDescriptor)

  // connect feature stream with cluster broadcast, classify featurized comments
  val classifiedComments: DataStream[ClassifiedEvent] =
    aggregatedFeaturesStream
      .keyBy(_.personId)
      .connect(broadcast)
      .process(new ClassifyEventsFunction(clusterStateDescriptor)).name("classify comments")

  // clusters.map(r => (r._1, r._2, r._3.clusters.map(c => (c.index, c.weight)))).print
  // classifiedComments.map(e => s"person: ${e.personId}\tcomment: ${e.eventId}\t-> ${e.cluster.index} (${e.cluster.weight})\t(${e.cluster.centroid})").print

  // write classification result to kafka/elasticsearch
  // TODO via kafka?
  classifiedComments
    .addSink(ElasticSearchIndexes.classification.createSink(1))
    .name(s"elastic search: ${ElasticSearchIndexes.classification.indexName}")

  env.execute()

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
      // TODO do this in windowing function to allow normalization/standardizaation?

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




package org.mvrs.dspa.clustering

import com.google.common.base.Splitter
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.{streams, utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

object UnusualActivityDetectionJob extends App {
  // TODO
  // - add side output stream and/or metrics on cluster evolution
  //   - maximum cluster movement distance? cluster index for maximum?
  // - allow controlling replay speed
  //   - read directly from csv?
  //   - or read from kafka, with wrapped source?
  //   - or read from kafka, with scaledreplayfunction?
  // - add textfile-based control stream to control
  //   - K
  //   - which clusters to flag as "unusual"
  // - use connect instead of join for connecting to frequency?
  // - create custom trigger that fires at end of window with count-based early firing
  // - if a cluster gets too small, split the largest cluster
  // - come up with better text features
  // - write additional information to ElasticSearch to help interpretation of activity classification
  // - refactor index classes (instances with base class)
  val elasticHostName = "localhost"
  val elasticPort = 9200
  val elasticScheme = "http"
  val elasticSearchUri = s"$elasticScheme://$elasticHostName:$elasticPort"
  val indexName = "activity-classification"
  val typeName = "activity-classification-type"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, indexName) // testing: recreate the index
    ActivityClassificationIndex.create(client, indexName, typeName)
  }
  finally {
    client.close()
  }

  // set up clustering stream:
  // - union of rooted comments and posts
  // - extract features from each event --> (person-id, Vector[Double])
  // - gather events on a single worker, in a tumbling window with trigger at maximum
  // - do k-means at end of window (merging with previous cluster centers)
  // - broadcast resulting clusters
  // - NOTE: previous clusters are used as seed points for new clusters --> new clusters mapped to old clusters simply by cluster index
  // - side output: cluster center difference
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(false) // use arg (scallop?)

  env.setParallelism(4) // NOTE with multiple workers, the comments AND broadcast stream watermarks lag VERY much behind
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val comments = streams.comments("activity-detection")

  val frequencyStream: DataStream[(Long, Int)] =
    comments
      .map(c => (c.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(12), Time.hours(1))
      .sum(1).name("calculate comment frequency per user")

  val commentFeaturesStream =
    comments
      .keyBy(_.personId)
      .map(c => (c.personId, c.id, extractFeatures(c))).name("extract comment features")

  // TODO use connect instead of join (and store frequency in value state), to get the *latest* per-user frequency at each comment?
  val featurizedComments: DataStream[(Long, Long, mutable.ArrayBuffer[Double])] =
    commentFeaturesStream
      .join(frequencyStream)
      .where(_._1) // person id
      .equalTo(_._1) // person id
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .apply { (t1, t2) => {
        // append per-user frequency to (mutable) feature vector
        t1._3.append(t2._2.toDouble)

        // return tuple
        (
          t1._1, // person id
          t1._2, // comment id
          t1._3, // feature vector
        )
      }
      }.name("aggregate features")

  // cluster combined features (on a single worker)
  // To parallelize: distribute points randomly, cluster subsets, merge resulting clusters as in
  // 7.6.4 of "Mining of massive datasets"
  val clusters: DataStream[(Long, Int, ClusterModel)] = featurizedComments
    .map(_._3)
    .keyBy(_ => 0)
    .timeWindow(Time.hours(24)) // update clusters at least once a day
    .trigger(CountTrigger.of[TimeWindow](2000L)) // TODO remainder in window lost, need combined end-of-window + early-firing trigger
    .process(new KMeansClusterFunction(k = 4, decay = 0.0)).name("calculate clusters").setParallelism(1)

  // broadcast stream
  val clusterStateDescriptor = new MapStateDescriptor(
    "ClusterBroadcastState",
    createTypeInformation[Int],
    createTypeInformation[(Long, Int, ClusterModel)])

  val broadcast = clusters.broadcast(clusterStateDescriptor)

  // connect feature stream with cluster broadcast, classify featurized comments
  val classifiedComments =
    featurizedComments
      .keyBy(_._1)
      .connect(broadcast)
      .process(new ClassifyEventsFunction(clusterStateDescriptor)).name("classify comments")

  // TODO use an additional control stream to identify clusters that should be reported / others that can be ignored
  // - another broadcast stream?

  // clusters.map(r => (r._1, r._2, r._3.clusters.map(c => (c.index, c.weight)))).print
  // classifiedComments.map(e => s"person: ${e.personId}\tcomment: ${e.eventId}\t-> ${e.cluster.index} (${e.cluster.weight})\t(${e.cluster.centroid})").print

  // TODO write classification result to kafka/elasticsearch
  classifiedComments.addSink(ActivityClassificationIndex.createSink(elasticHostName, elasticPort, elasticScheme, indexName, typeName))

  env.execute()

  def extractFeatures(comment: CommentEvent): mutable.ArrayBuffer[Double] = {
    val tokens = comment.content
      .map(tokenize(_).toVector)
      .getOrElse(Vector.empty[String])

    val dim = 3
    val buffer = new mutable.ArrayBuffer[Double](dim)

    if (tokens.isEmpty) buffer ++= List.fill(dim)(0.0) // zero vector
    else {
      // TODO how to scale/normalize features?
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

  def tokenize(str: String) = {
    splitter.split(str).asScala
  }

}





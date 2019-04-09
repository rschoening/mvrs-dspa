package org.mvrs.dspa.clustering

import com.google.common.base.Splitter
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.mvrs.dspa.clustering.KMeansClustering.Point
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.{streams, utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

object UnusualActivityDetectionJob extends App {
  // set up clustering stream:
  // - union of rooted comments and posts
  // - extract features from each event --> (person-id, Vector[Double])
  // - gather events on a single worker, in a tumbling window with trigger at maximum
  // - do k-means at end of window (merging with previous cluster centers)
  // - broadcast resulting clusters
  // - NOTE: previous clusters are used as seed points for new clusters --> new clusters mapped to old clusters simply by cluster index
  // - side output: cluster center difference
  implicit val env: StreamExecutionEnvironment = utils.createStreamExecutionEnvironment(true) // use arg (scallop?)

  env.setParallelism(4) // NOTE with multiple workers, the comments AND broadcast stream watermarks lag VERY much behind
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val comments = streams.comments("activity-detection")

  val frequencyStream: DataStream[(Long, Int)] =
    comments
      .map(c => (c.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(12), Time.hours(1))
      .sum(1).name("calculate comment frequency per user")

  //   frequencyStream.print

  val commentFeaturesStream =
    comments
      .keyBy(_.personId)
      .map(c => (c.personId, c.id, extractFeatures(c))).name("extract comment features")

  // TODO use connect to get the *latest* frequency at each comment?
  val featurizedComments: DataStream[(Long, Long, mutable.ArrayBuffer[Double])] =
    commentFeaturesStream
      .join(frequencyStream)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .apply { (t1, t2) => {
        // append per-user frequency to feature vector
        t1._3.append(t2._2.toDouble)
        (
          t1._1, // person id
          t1._2, // comment id
          t1._3, // feature vector
        )
      }
      }.name("aggregate features")

  // - process featureized comments in clustering stream, broadcast clusters periodically back to the same stream
  // - connect with cluster stream, classify featurized comments

  val clusters: DataStream[(Long, Int, Seq[Cluster])] = featurizedComments
    .map(_._3)
    .keyBy(_ => 0)
    .timeWindow(Time.hours(24))
    .trigger(CountTrigger.of[TimeWindow](1000L)) // TODO remainder in window lost, need combined end-of-window + early-firing trigger
    .process(new KMeansClusterFunction(k = 4)).name("calculate clusters").setParallelism(1)

  // clusters.print()

  val clusterStateDescriptor = new MapStateDescriptor(
    "ClusterBroadcastState",
    createTypeInformation[Int],
    createTypeInformation[(Long, Int, Seq[Cluster])])

  val broadcast = clusters.broadcast(clusterStateDescriptor)

  val classifiedComments =
    featurizedComments
      .keyBy(_._1)
      .connect(broadcast)
      .process(new ClassifyEventsFunction(clusterStateDescriptor)).name("classify comments")

  // TODO use an additional control stream to identify clusters that should be reported / others that can be ignored
  // - another broadcast stream?

  classifiedComments.print

  env.execute()

  //  val joinedStream = commentFeaturesStream.join()

  // set up detection stream:
  // - union of rooted comments and posts
  // - key by person-id
  // - extract features for events
  // - assign to nearest cluster
  // ?? which cluster to report on?

  case class Cluster(index: Int, centroid: Point, size: Int)

  def extractFeatures(comment: CommentEvent): mutable.ArrayBuffer[Double] = {
    val tokens: Seq[String] = comment.content.map(str => tokenize(str).toVector).getOrElse(Vector.empty[String])

    val buffer = new mutable.ArrayBuffer[Double](2)

    if (tokens.isEmpty) buffer.append(0, 0, 0, 0)
    else {
      buffer += math.log(tokens.size)
      buffer += math.log(tokens.map(_.toLowerCase()).distinct.size) // distinct word count
      buffer += tokens.count(_.forall(_.isUpper)) / tokens.size // % of all-UPPERCASE words
      buffer += tokens.count(_.length == 4) / tokens.size // % of four-letter words
    }
    buffer
  }

  private lazy val splitter =
    Splitter
      .onPattern("[\\s,.;]+")
      .omitEmptyStrings()
      .trimResults()

  def tokenize(str: String) = {
    splitter.split(str).asScala
  }

}





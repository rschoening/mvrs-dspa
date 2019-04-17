package functions

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.api.common.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.jobs.activeposts.{Event, EventType, PostStatisticsFunction}
import org.mvrs.dspa.utils
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

/**
  * Integration test suite for [[org.mvrs.dspa.jobs.activeposts.PostStatisticsFunction]]
  */
class PostStatisticsFunctionITSuite extends AbstractTestBase {

  @Test
  def testSliding(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    // create a stream of custom elements and apply transformations
    // TODO consider property-based testing using event generator
    val events: List[Event] = List(
      Event(EventType.Comment, postId = 1, personId = 100, timestamp = 10),
      Event(EventType.Post, postId = 2, personId = 100, timestamp = 20),
      Event(EventType.Reply, postId = 1, personId = 200, timestamp = 30),
      Event(EventType.Post, postId = 2, personId = 200, timestamp = 100),
      Event(EventType.Post, postId = 1, personId = 100, timestamp = 500),
    )

    val maxOutofOrderness = Time.milliseconds(100)

    val stream = env.fromCollection(events)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[Event](maxOutofOrderness, _.timestamp))
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(11, 5))

    env.execute()

    // Note: this must be called *after* execute()
    val statistics = DataStreamUtils.collect(stream.javaStream).asScala.toList
    statistics.foreach(println(_))

    // TODO assertions
  }

  @Test
  def testTumbling(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    // create a stream of custom elements and apply transformations

    // TODO check if property-based testing can somehow be used in conjunction with Flink's AbstractTestBase
    val events: List[Event] = List(
      Event(EventType.Comment, postId = 1, personId = 100, timestamp = 0),
      Event(EventType.Comment, postId = 2, personId = 100, timestamp = 0),
      Event(EventType.Reply, postId = 1, personId = 200, timestamp = 30),
      Event(EventType.Reply, postId = 1, personId = 200, timestamp = 30),
      Event(EventType.Comment, postId = 2, personId = 100, timestamp = 60),
      Event(EventType.Comment, postId = 2, personId = 200, timestamp = 100),
      Event(EventType.Reply, postId = 1, personId = 100, timestamp = 500),
      Event(EventType.Reply, postId = 1, personId = 300, timestamp = 600),
    )

    val maxOutofOrderness = Time.milliseconds(1)

    val stream = env.fromCollection(events)
      // .process(new ScaledReplayFunction[Event](_.timestamp, 0.01, 0)) // NOTE this causes assertion violation below - function is not finished
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[Event](maxOutofOrderness, _.timestamp))
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(20, 20))

    // Note: this must be called *instead of* execute()
    val statistics = DataStreamUtils.collect(stream.javaStream).asScala.toList

    statistics.foreach(println(_))

    assertResult(events.count(_.eventType == EventType.Reply))(statistics.map(_.replyCount).sum)
    assertResult(events.count(_.eventType == EventType.Comment))(statistics.map(_.commentCount).sum)
  }

  @Test
  def testSingleWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    // create a stream of custom elements and apply transformations

    // TODO check if property-based testing can somehow be used in conjunction with Flink's AbstractTestBase
    val events: List[Event] = List(
      Event(EventType.Comment, postId = 1, personId = 100, timestamp = 0),
      Event(EventType.Comment, postId = 2, personId = 100, timestamp = 0),
      Event(EventType.Reply, postId = 1, personId = 200, timestamp = 30),
      Event(EventType.Reply, postId = 1, personId = 200, timestamp = 30),
      Event(EventType.Comment, postId = 2, personId = 100, timestamp = 60),
      Event(EventType.Comment, postId = 2, personId = 200, timestamp = 100),
      Event(EventType.Reply, postId = 1, personId = 100, timestamp = 500),
      Event(EventType.Reply, postId = 1, personId = 300, timestamp = 600),
    )

    val maxOutofOrderness = Time.milliseconds(1)

    val stream = env.fromCollection(events)
      // .process(new ScaledReplayFunction[Event](_.timestamp, 0.01, 0)) // NOTE this causes assertion violation below - function is not finished, but no obvious cause found yet
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[Event](maxOutofOrderness, _.timestamp))
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(1000, 1000))

    // Note: this must be called *instead of* execute()
    val statistics = DataStreamUtils.collect(stream.javaStream).asScala.toList

    statistics.foreach(println(_))

    // check the event counts
    assertResult(events.count(_.eventType == EventType.Reply))(statistics.map(_.replyCount).sum)
    assertResult(events.count(_.eventType == EventType.Comment))(statistics.map(_.commentCount).sum)

    // check the distinct users; must group by post to avoid double-counting
    events.groupBy(_.postId).foreach {
      case (postId, eventsForPost) =>
        assertResult(eventsForPost.map(_.personId).toSet.size)(statistics.filter(_.postId == postId).map(_.distinctUserCount).sum)
    }
  }
}

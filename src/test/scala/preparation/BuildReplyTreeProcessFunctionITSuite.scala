package preparation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.preparation.BuildCommentsHierarchyJob
import org.mvrs.dspa.utils
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters._

/**
  * Integration test suite for [[org.mvrs.dspa.preparation.BuildReplyTreeProcessFunction]]
  */
class BuildReplyTreeProcessFunctionITSuite extends AbstractTestBase {

  @Test
  def testOrderedInput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.setParallelism(1)

    val postId = 999
    val personId = 1
    val rawComments: List[CommentEvent] = List(
      CommentEvent(id = 111, personId, creationDate = 1000, None, None, None, Some(postId), None, 0),
      CommentEvent(id = 112, personId, creationDate = 2000, None, None, None, None, Some(111), 0),
      CommentEvent(id = 113, personId, creationDate = 3000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 114, personId, creationDate = 4000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 115, personId, creationDate = 5000, None, None, None, None, Some(113), 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](Time.milliseconds(100), _.creationDate))

    val (rootedStream, _) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    // Note: this must be called *instead of* execute(), once for each stream
    val rooted = DataStreamUtils.collect(rootedStream.javaStream).asScala.toList

    println("rooted:")
    println(rooted.mkString("\n"))

    assertResult(rawComments.length)(rooted.length)
    assert(rooted.forall(_.postId == postId))
  }

  @Test
  def testUnorderedInput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.setParallelism(1)

    val postId = 999
    val personId = 1
    val rawComments: List[CommentEvent] = List(
      CommentEvent(id = 114, personId, creationDate = 1000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 115, personId, creationDate = 1000, None, None, None, None, Some(113), 0),
      CommentEvent(id = 112, personId, creationDate = 3000, None, None, None, None, Some(111), 0),
      CommentEvent(id = 113, personId, creationDate = 4000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 111, personId, creationDate = 5000, None, None, None, Some(postId), None, 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](Time.milliseconds(100), _.creationDate))

    val (rootedStream, _) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    // Note: this must be called *instead of* execute()
    val rooted = DataStreamUtils.collect(rootedStream.javaStream).asScala.toList

    println("rooted:")
    println(rooted.mkString("\n"))

    assertResult(rawComments.length)(rooted.length)
    assert(rooted.forall(_.postId == postId))
  }


  @Test
  def dropDanglingReplies(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.setParallelism(4)

    // NOTE it seems that events are broadcasted *more* than once per worker, at least in this test context
    //      (maybe an influence of collect() method below?)
    val postId = 999
    val personId = 1
    val rawComments: List[CommentEvent] = List(
      CommentEvent(id = 114, personId, creationDate = 1000, None, None, None, None, Some(112), 0),
      CommentEvent(id = 115, personId, creationDate = 1000, None, None, None, None, Some(113), 0), // child of dangling parent
      CommentEvent(id = 112, personId, creationDate = 3000, None, None, None, None, Some(111), 0),
      CommentEvent(id = 113, personId, creationDate = 4000, None, None, None, None, Some(888), 0), // dangling, unknown parent
      CommentEvent(id = 111, personId, creationDate = 5000, None, None, None, Some(postId), None, 0),
    )

    val stream = env.fromCollection(rawComments)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](Time.milliseconds(100), _.creationDate))

    val (_, droppedStream) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    // Note: this must be called *instead of* execute()
    val dropped = DataStreamUtils.collect(droppedStream.javaStream).asScala.toList

    println("dropped:")
    println(dropped.mkString("\n"))

    val danglingIds = List(113, 115)
    assertResult(2)(dropped.length)
    assert(dropped.forall(_.replyToPostId.isEmpty))
    assert(dropped.forall(r => danglingIds.contains(r.id)))
  }
}

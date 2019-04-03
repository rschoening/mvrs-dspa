package preparation

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.preparation.BuildCommentsHierarchyJob
import org.mvrs.dspa.utils
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters._
import scala.collection.mutable

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

    val (rootedStream, droppedStream) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values
    val dropped = DroppedSink.values

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

    assertResult(0)(dropped.length)
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

    val (rootedStream, droppedStream) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values
    val dropped = DroppedSink.values

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

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

    val (rootedStream, droppedStream) = BuildCommentsHierarchyJob.resolveReplyTree(stream)

    RootedSink.values.clear()
    DroppedSink.values.clear()
    rootedStream.addSink(new RootedSink)
    droppedStream.addSink(new DroppedSink)

    env.execute()

    val rooted = RootedSink.values
    val dropped = DroppedSink.values

    println("rooted:")
    println(rooted.mkString("\n"))

    println("dropped:")
    println(dropped.mkString("\n"))

    assertResult(3)(rooted.length)
    val danglingIds = List(113, 115)
    assertResult(2)(dropped.length)
    assert(dropped.forall(_.replyToPostId.isEmpty))
    assert(dropped.forall(r => danglingIds.contains(r.id)))
  }

}

// sinks: see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html
// NOTE: these classes must not be nested in the test class, otherwise they are not serializable

class RootedSink extends SinkFunction[CommentEvent] {
  override def invoke(value: CommentEvent): Unit =
    synchronized {
      RootedSink.values += value
    }
}

object RootedSink {
  val values: mutable.MutableList[CommentEvent] = mutable.MutableList[CommentEvent]() // must be static
}

class DroppedSink extends SinkFunction[CommentEvent] {
  override def invoke(value: CommentEvent): Unit =
    synchronized {
      DroppedSink.values += value
    }
}

object DroppedSink {
  val values: mutable.MutableList[CommentEvent] = mutable.MutableList[CommentEvent]() // must be static
}

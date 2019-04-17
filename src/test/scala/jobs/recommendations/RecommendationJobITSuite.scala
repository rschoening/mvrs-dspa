package jobs.recommendations

import java.time.{LocalDateTime, ZoneOffset}
import java.util
import java.util.Date

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.events.ForumEvent
import org.mvrs.dspa.jobs.recommendations.RecommendationsJob
import org.mvrs.dspa.utils
import org.scalatest.Assertions._

import scala.collection.JavaConverters._

class RecommendationJobITSuite extends AbstractTestBase {
  val personA = 1
  val personB = 2
  val personC = 3
  val post1 = 100
  val post2 = 200
  val post3 = 300

  @Test
  def collectPostsInSingleWindow(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val posts = RecommendationsJob
      .collectPostsInteractedWith(
        forumEventStream(env, List(
          Event(personA, post1, time(10, 0)),
          Event(personB, post1, time(11, 0)),
          Event(personB, post2, time(12, 0)),
          Event(personA, post1, time(13, 0)),
          Event(personC, post1, time(14, 0)),
          Event(personB, post1, time(15, 0)),
          Event(personC, post2, time(16, 0)),
          Event(personC, post3, time(17, 0)),
        )),
        Time.hours(24),
        Time.hours(24))

    posts.addSink(new PostsInWindowSink())

    env.execute("test")

    val result = PostsInWindowSink.values.asScala.toMap

    assertResult(Set(post1))(result(personA))
    assertResult(Set(post1, post2))(result(personB))
    assertResult(Set(post1, post2, post3))(result(personC))
  }

  private def forumEventStream(env: StreamExecutionEnvironment, events: Seq[ForumEvent]) = {
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0) // deactivate the watch dog to allow stress-free debugging
    env.getConfig.setAutoWatermarkInterval(100L) // required for watermarks and correct timers
    env.fromCollection(events)
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[ForumEvent](
        Time.minutes(1), extract = _.timestamp))
  }

  def time(hour: Int, minutes: Int): Date =
    Date.from(LocalDateTime.of(2000, 12, 30, hour, minutes).toInstant(ZoneOffset.UTC))

  def synchronizedList[T](): util.Collection[T] = util.Collections.synchronizedCollection(new util.ArrayList[T])

  case class Event(personId: Long, postId: Long, creationDate: java.util.Date) extends ForumEvent

}

class PostsInWindowSink extends SinkFunction[(Long, Set[Long])] {
  override def invoke(value: (Long, Set[Long])): Unit = PostsInWindowSink.values.add(value)
}

object PostsInWindowSink {
  val values: util.Collection[(Long, Set[Long])] = util.Collections.synchronizedCollection(new util.ArrayList[(Long, Set[Long])])
}


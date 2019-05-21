package streams

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.streams
import org.mvrs.dspa.utils.DateTimeUtils
import org.scalatest.Assertions._
import utils.TestUtils

import scala.collection.JavaConverters._

class InputStreamsITSuite extends AbstractTestBase {
  @Test
  def testReadingLikes(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(4)

    val startTime = System.currentTimeMillis()

    streams
      .likesFromCsv(TestUtils.getResourceURIPath("/streams/likes.csv"))
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

    print(results)

    assertResult(117151)(results.map(_._2).sum) // like event count
    assertResult(26623)(results.map(_._1).distinct.size) // distinct posts
    assertResult(29741)(results.size) // count of per-post windows


    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.3 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingPosts(): Unit = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .postsFromCsv(TestUtils.getResourceURIPath("/streams/posts.csv"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

    print(results)

    assertResult(30956)(results.map(_._2).sum) // post event count
    assertResult(382)(results.map(_._1).distinct.size) // distinct persons
    assertResult(1266)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")

    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.2 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingRawComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .rawCommentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"))
      .map(e => (e.personId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

    print(results)

    assertResult(113422)(results.map(_._2).sum) // post event count
    assertResult(822)(results.map(_._1).distinct.size) // distinct persons
    assertResult(3880)(results.size) // count of per-person windows

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")

    // durations:
    // - parallelism = 4: 3.3 sec (note: source is non-parallel)
    // - parallelism = 1: 3.2 sec
    // -> dominated by init/teardown overhead, data volume too small to benefit from parallelism
  }

  @Test
  def testReadingComments(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .commentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"), 10000000)
      .map(e => (e.postId, 1))
      .keyBy(_._1)
      .timeWindow(Time.days(30))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .addSink(new CollectionSink)

    CollectionSink.values.clear()

    env.execute()

    val results = CollectionSink.values.asScala.toList

    print(results)

    assertResult(11575)(results.map(_._1).distinct.size) // distinct posts

    val duration = System.currentTimeMillis() - startTime

    println(s"duration: ${DateTimeUtils.formatDuration(duration)}")
  }

  @Test
  def testReadingCommentsNonWindowed(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.setParallelism(1)

    val startTime = System.currentTimeMillis()

    streams
      .commentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"))
      .map(e => (e.postId, 1))
      .addSink(new CounterSink[(Long, Int)])

    CounterSink.counter.set(0)

    env.execute()

    val result = CounterSink.counter.get()

    println(result)

    println(s"duration: ${DateTimeUtils.formatDuration(System.currentTimeMillis() - startTime)}")
  }

  @Test
  def testCompareComments(): Unit = {
    val result1 = getCommentIds
    val result2 = getCommentIds

    println(s"result size 1: ${result1.size} - 2: ${result2.size}")
    println(s"1 - 2: ${result1 -- result2}")
    println(s"2 - 1: ${result2 -- result1}")

    /*
    some examples:

    result size 1: 107216 - 2: 107228
      1 - 2: Set(236620, 236650)
      2 - 1: Set(260610, 4984730, 4984350, 2740070, 1045560, 3113890, 3089610, 859410, 2347400, 1045540, 2347390, 859380, 3644180, 260620)

    result size 1: 107232 - 2: 107221
      1 - 2: Set(236620, 4944550, 4984730, 4944580, 2554680, 3376590, 151800, 4944480, 2554600, 4944500, 236650, 2554640, 151740, 3644180, 2554660)
      2 - 1: Set(4984350, 5832060, 859410, 859380)


      element: 236620
      ---------------------------------------------------------
      | id     | uid | creation date        | post  | parent  |
      ---------------------------------------------------------
   -> | 236570 | 825 | 2012-03-18T04:11:43Z | 54520 |   -     | (1)  parent -> post (line 9385)
   ** | 236620 | 109 | 2012-03-18T04:56:00Z |   -   | 236590  | (2)  grandchild     (line 9412, + 45 minutes)
   -> | 236590 | 642 | 2012-03-18T07:38:42Z |   -   | 236570  | (3)  child          (line 9494, + 2h 42 minutes)
      ---------------------------------------------------------
     ==> 236620 *should* be dropped, since its parent is late (but is sometimes emitted)

     element: 236650
      ---------------------------------------------------------
      | id     | uid | creation date        | post  | parent  |
      ---------------------------------------------------------
   -> | 236570 | 825 | 2012-03-18T04:11:43Z | 54520 |    -    | (1)  parent -> post   (line 9385)
   -> | 236620 | 109 | 2012-03-18T04:56:00Z |   -   | 236590  | (3)  grandchild       (line 9412)
   -> | 236590 | 642 | 2012-03-18T07:38:42Z |   -   | 236570  | (4)  child            (line 9494, + 2h 42 minutes)
   ** | 236650 | 102 | 2012-03-19T02:14:21Z |   -   | 236620  | (2)  great-grandchild (line 9749)
      ---------------------------------------------------------
    children of 236590: 236640, 236620


    after updates to eviction logic:
    --------------------------------

    result size 1: 107167 - 2: 107171
    1 - 2: Set(151790, 3109980, 2922510, 151810, 5965010, 2360240, 151800, 151760, 1390200, 621890, 3571910, 621870, 151740, 151820, 3644180, 1157800, 3402140)
    2 - 1: Set(1908210, 875970, 875930, 876010, 875910, 1908110, 3517230, 1045560, 110110, 1908130, 3429410, 1366870, 1908190, 1366890, 4650940, 1439290, 3429340, 3517110, 5034250, 3517210, 1908170)

    result size 1: 107152 - 2: 107172
    1 - 2: Set(5832060, 5965010, 3571490, 3571500, 3571510, 1157800, 3402140, 3571530)
    2 - 1: Set(1908210, 875970, 875930, 424150, 876010, 875910, 1908110, 3517230, 1045560, 110110, 1908130, 3429410, 3571870, 1366870, 1908190, 1366890, 4650940, 1439290, 3429340, 3571880, 1908100, 3517110, 5970120, 5034250, 3571900, 423400, 3517210, 1908170)

    result size 1: 107163 - 2: 107170
    1 - 2: Set(3109980, 2360240, 1157800)
    2 - 1: Set(875970, 875930, 876010, 875910, 1045560, 110110, 3571870, 4650940, 3571880, 3571900)

     element: 875910

     875910|377|2012-02-27T08:28:22Z|110.5.96.18|Internet Explorer|About Jacques Chirac, his method. Then, his economic policies, based on dirigisme,. About Amitabh Bachchan, both the Padma Shri and the Padma Bhushan civilian awards.||875890|39
     875890|232|2012-02-27T00:24:04Z|27.124.0.17|Internet Explorer|About Jacques Chirac, (two full terms, the first of seven years and the second of five years), after Fran�ois Mitterrand. As. About Amitabh Bachchan, first gained popularity in the early 1970s as the angry young man of Hindi cinema, and has since appeared in.||875870|38
     875930|870|2012-02-28T00:31:07Z|77.95.0.9|Chrome|About Jacques Chirac, the second of five years), after Fran�ois Mitterrand. As. About Amitabh Bachchan, Harivansh Bachchan on 11 October 1942) is an Indian film.||875910|97
     875970|410|2012-02-29T01:23:17Z|46.245.0.1|Firefox|About Jacques Chirac, the French L�gion d'honneur. On 15 December 2011, the Paris court. About Amitabh Bachchan, on 11 October 1942) is an Indian film actor. He first gained.||875930|40
     876010|702|2012-03-01T00:28:28Z|14.144.0.10|Firefox|About Jacques Chirac, the laissez-faire policies of the United Kingdom, which Chirac. About Amitabh Bachchan, as the angry young man of Hindi cinema, and has since appeared.||875970|73
     875870|365|2012-02-27T00:44:50Z|103.4.52.6|Chrome|About Jacques Chirac, of healing the social rift (fracture sociale). About Amitabh Bachchan, 1980s. He has received both the Padma Shri and the.|200600||39

      ------------------------------------------------------------
      | id     | uid | creation date        | post   | parent    |
      ------------------------------------------------------------
      | 875890 |     | 2012-02-27T00:24:04Z |        | 875870  v | reference to later - must ALWAYS be discarded - and apparently IS
      | 875870 |     | 2012-02-27T00:44:50Z | 200600 |           | single child
   ** | 875910 |     | 2012-02-27T08:28:22Z |        | 875890  ^ | reference to discarded reply - sometimes not discarded
   ** | 875930 |     | 2012-02-28T00:31:07Z |        | 875910  ^ |
   ** | 875970 |     | 2012-02-29T01:23:17Z |        | 875930  ^ |
   ** | 876010 |     | 2012-03-01T00:28:28Z |        | 875970  ^ | 876010 is leaf
      ------------------------------------------------------------

      Set(875890, 875870, 875910, 875930, 875970, 876010)

observed executions:

processBroadcastElement(875890)
processElement(875870)
processBroadcastElement(875910)
processBroadcastElement(875930)
processBroadcastElement(875970)
processBroadcastElement(876010)

processBroadcastElement(875890)
processBroadcastElement(875910)
processBroadcastElement(875930)
processElement(875870)
processBroadcastElement(875970)
processBroadcastElement(876010)

Differences:
result size 1: 107163 - 2: 107164
1 - 2: Set(3429410, 1366870, 1366890, 3429340, 1885810, 5034250, 1157800)
2 - 1: Set(875970, 875930, 876010, 875910, 1045560, 3571870, 3571880, 3571900)

processBroadcastElement(875890)
processElement(875870)
processBroadcastElement(875910)
processBroadcastElement(875930)
processBroadcastElement(875970)
processBroadcastElement(876010)

processElement(875870)
processBroadcastElement(875890)
processBroadcastElement(875910)
processBroadcastElement(875930)
processBroadcastElement(875970)
processBroadcastElement(876010)

result size 1: 107145 - 2: 107145
1 - 2: Set(110110, 423400)
2 - 1: Set(1885810, 1157800)

   */

  }

  private def getCommentIds: Set[Long] = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setTaskCancellationTimeout(0)
    env.setParallelism(1)

    CollectionSink.values.clear()
    streams
      .commentsFromCsv(TestUtils.getResourceURIPath("/streams/comments.csv"), watermarkInterval = 100)
      .map(e => (e.commentId, 1))
      .addSink(new CollectionSink())

    env.execute()

    CollectionSink.values.asScala.map(_._1).toSet
  }

  private def print(results: List[(Long, Int)]): Unit = {
    println(results.map(_._2).sum) // like event count
    println(results.map(_._1).distinct.size) // distinct posts
    println(results.size) // count of per-post windows
  }
}

class CounterSink[T] extends SinkFunction[T] {
  override def invoke(value: T): Unit = CounterSink.counter.incrementAndGet()
}

object CounterSink {
  val counter: AtomicLong = new AtomicLong(0)
}

class CollectionSink extends SinkFunction[(Long, Int)] {
  override def invoke(value: (Long, Int)): Unit = CollectionSink.values.add(value)
}

object CollectionSink {
  // NOTE using
  // synchronized { /* access to non-threadsafe collection */ }
  // does not work, collection still corrupt --> Flink documentation should be changed
  val values: util.Collection[(Long, Int)] = Collections.synchronizedCollection(new util.ArrayList[(Long, Int)])
}
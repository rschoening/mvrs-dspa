package db

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.experimental.categories.Category
import org.junit.{Ignore, Test}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.ActivePostStatisticsIndex
import org.mvrs.dspa.model.PostStatistics

@Category(Array(classOf[categories.ElasticSearchTests]))
class ElasticSearchSinkITSuite extends AbstractTestBase {

  @Ignore("requires ElasticSearch")
  @Test
  def baselinePerformance(): Unit = {
    val index = new ActivePostStatisticsIndex("mvrs-test-active-post-statistics", Settings.elasticSearchNodes: _*)
    index.create()

    // set up the streaming execution environment
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    env.generateSequence(1, 10000)
      .map(i => (PostStatistics(i, System.currentTimeMillis, 1, 1, 1, 3, newPost = false), "test", "testforum"))
      .addSink(index.createSink(100))

    val startTime = System.currentTimeMillis

    // execute program
    env.execute("Move post statistics from Kafka to ElasticSearch")

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime

    println(s"Duration: $duration ms")
    println(env.getExecutionPlan)
  }
}

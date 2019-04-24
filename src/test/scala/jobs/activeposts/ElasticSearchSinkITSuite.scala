package jobs.activeposts

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mvrs.dspa.Settings
import org.mvrs.dspa.jobs.activeposts.{ActivePostStatisticsIndex, PostStatistics}

@Category(Array(classOf[categories.ElasticSearchTests]))
class ElasticSearchSinkITSuite extends AbstractTestBase {

  @Test
  def baselinePerformance(): Unit = {
    val index = new ActivePostStatisticsIndex("post-statistics_test", Settings.elasticSearchNodes(): _*)
    index.create()

    // set up the streaming execution environment
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    env.generateSequence(1, 10000)
      .map(i => PostStatistics(i, System.currentTimeMillis, 1, 1, 1, 3, newPost = false))
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

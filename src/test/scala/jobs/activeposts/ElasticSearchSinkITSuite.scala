package jobs.activeposts

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.io.ElasticSearchUtils
import org.mvrs.dspa.jobs.activeposts.{ActivePostStatisticsIndex, PostStatistics}

class ElasticSearchSinkITSuite extends AbstractTestBase {

  @Test
  def baselinePerformance(): Unit = {
    val elasticHostName = "localhost"
    val elasticPort = 9200
    val elasticScheme = "http"
    val elasticSearchUri = s"$elasticScheme://$elasticHostName:$elasticPort"
    val indexName = "poststatistics_test"
    val typeName = "poststatistics_type_test"

    val client = ElasticClient(ElasticProperties(elasticSearchUri))
    try {
      ElasticSearchUtils.dropIndex(client, indexName) // testing: recreate the index
      ActivePostStatisticsIndex.create(client, indexName, typeName)
    }
    finally {
      client.close()
    }

    // set up the streaming execution environment
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    env.generateSequence(1, 10000)
      .map(i => PostStatistics(i, System.currentTimeMillis, 1, 1, 1, 3, newPost = false))
      .addSink(ActivePostStatisticsIndex.createSink(elasticHostName, elasticPort, elasticScheme, indexName, typeName))

    val startTime = System.currentTimeMillis

    // execute program
    env.execute("Move post statistics from Kafka to ElasticSearch")

    val endTime = System.currentTimeMillis
    val duration = endTime - startTime

    println(s"Duration: $duration ms")
    println(env.getExecutionPlan)
  }
}

package jobs.recommendations

import org.apache.flink.streaming.api.scala._
import org.junit.Test
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.jobs.recommendations.RecommendationsIndex

class RecommendationSinkFunctionITSuite {
  @Test
  def testUpsertToElasticSearch(): Unit = {

    // NOTE this requires elasticsearch to run on localhost:9200
    val elasticHostName = "localhost"
    val indexName = "recommendations_test"
    val typeName = "recommendations_test_type"

    val index = new RecommendationsIndex(indexName, typeName, ElasticSearchNode(elasticHostName))
    index.create()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val generated: DataStream[(Long, scala.Seq[(Long, Double)])] = env
      .generateSequence(1L, 1000L)
      .map(v => (v, (0L to 10L).map(u => (u, 0.5))))

    generated.addSink(index.createSink(batchSize = 100))

    env.execute("test")
  }
}

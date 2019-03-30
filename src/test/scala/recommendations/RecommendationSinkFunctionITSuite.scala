package recommendations

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.streaming.api.scala._
import org.junit.Test
import org.mvrs.dspa.recommendations.RecommendationsIndex
import org.mvrs.dspa.utils

class RecommendationSinkFunctionITSuite {
  @Test
  def testUpsertToElasticSearch(): Unit = {

    // NOTE this requires elasticsearch to run on localhost:9200

    val elasticHostName = "localhost"
    val elasticPort = 9200
    val elasticScheme = "http"
    val indexName = "recommendations_test"
    val typeName = "recommendations_test_type"
    val elasticSearchUri = s"$elasticScheme://$elasticHostName:$elasticPort"

    println(elasticSearchUri)
    val client = ElasticClient(ElasticProperties(elasticSearchUri))
    try {
      utils.dropIndex(client, indexName)
      RecommendationsIndex.create(client, indexName, typeName)
    }
    finally {
      client.close()
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val generated: DataStream[(Long, scala.Seq[(Long, Double)])] = env
      .generateSequence(1L, 1000L)
      .map(v => (v, (0L to 10L).map(u => (u, 0.5))))

    generated.addSink(
      RecommendationsIndex.createSink(
        elasticHostName, elasticPort, elasticScheme,
        indexName, typeName))

    env.execute("test")
  }
}

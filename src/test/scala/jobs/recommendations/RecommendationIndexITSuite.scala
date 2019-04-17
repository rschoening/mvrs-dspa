package jobs.recommendations

import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.get.GetResponse
import jobs.recommendations.RecommendationIndexITSuite._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.mvrs.dspa.io.{ElasticSearchNode, ElasticSearchUtils}
import org.mvrs.dspa.jobs.recommendations.RecommendationsIndex
import org.scalatest.Assertions._

class RecommendationIndexITSuite extends AbstractTestBase {
  @Test
  def testUpsertToElasticSearch(): Unit = {

    // NOTE this requires elasticsearch to run on localhost:9200
    val esNode = ElasticSearchNode("localhost")
    val indexName = "recommendations_test"
    val typeName = "recommendations_test_type"
    val index = new RecommendationsIndex(indexName, typeName, esNode)
    index.create()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val lastId = 1000L
    val recommendedCount = 10
    val generated: DataStream[(Long, scala.Seq[(Long, Double)])] = env
      .generateSequence(1L, lastId)
      .map(v => (v, (0L until recommendedCount).map(u => (u, similarity(u)))))

    generated.addSink(index.createSink(batchSize = 100))

    env.execute("test")

    // read back the last inserted record
    val client = ElasticSearchUtils.createClient(esNode)

    import com.sksamuel.elastic4s.http.ElasticDsl._
    val response: Response[GetResponse] = client.execute {
      get(lastId.toString) from indexName / typeName
    }.await

    // assert correct results
    assert(response.isSuccess)
    val sourceMap: Map[String, AnyRef] = response.result.sourceAsMap
    println(sourceMap)
    val users = sourceMap("users").asInstanceOf[List[Map[String, AnyRef]]]
    assertResult(recommendedCount)(users.size)

    users.zipWithIndex.forall {
      case (map, i) => map("uid").asInstanceOf[Int] == i && map("similarity").asInstanceOf[Double] == similarity(i)
    }
  }
}

object RecommendationIndexITSuite {
  private def similarity(uid: Long) = uid / 10.0
}

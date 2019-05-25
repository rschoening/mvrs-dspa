package db

import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.get.GetResponse
import db.RecommendationIndexITSuite._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.AbstractTestBase
import org.junit.experimental.categories.Category
import org.junit.{Ignore, Test}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.RecommendationsIndex
import org.mvrs.dspa.model.{Recommendation, RecommendedPerson}
import org.mvrs.dspa.utils.elastic
import org.scalatest.Assertions._

@Category(Array(classOf[categories.ElasticSearchTests]))
class RecommendationIndexITSuite extends AbstractTestBase {

  @Ignore("requires ElasticSearch")
  @Test
  def testUpsertToElasticSearch(): Unit = {

    // NOTE this requires elasticsearch to run on localhost:9200
    val index = new RecommendationsIndex("mvrs-test-recommendations", Settings.elasticSearchNodes: _*)
    index.create()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val lastId = 1000L
    val recommendedCount = 10
    val generated: DataStream[(Recommendation, Long)] = env
      .generateSequence(1L, lastId)
      .map(v => (Recommendation(v, (0L until recommendedCount).map(u => RecommendedPerson(u, similarity(u)))), 1000L))

    generated.addSink(index.createSink(batchSize = 100))

    env.execute("test")

    // read back the last inserted record
    val client = elastic.createClient(Settings.elasticSearchNodes: _*)

    import com.sksamuel.elastic4s.http.ElasticDsl._
    val response: Response[GetResponse] = client.execute {
      get(lastId.toString) from index.indexName / index.typeName
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

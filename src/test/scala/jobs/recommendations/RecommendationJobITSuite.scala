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

class RecommendationJobITSuite extends AbstractTestBase {

}

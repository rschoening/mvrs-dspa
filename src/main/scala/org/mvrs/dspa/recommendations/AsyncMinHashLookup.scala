package org.mvrs.dspa.recommendations

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.events.ForumEvent
import org.mvrs.dspa.io.AsyncElasticSearchFunction

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncMinHashLookup(elasticSearchUri: String, minHasher: MinHasher32)
  extends AsyncElasticSearchFunction[ForumEvent, (Long, MinHashSignature)](elasticSearchUri) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient, input: ForumEvent, resultFuture: ResultFuture[(Long, MinHashSignature)]): Unit = {
    client.execute {
      get(input.personId.toString).from("recommendation_person_features" / "recommendation_person_features_type")
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  // TODO do this outside for testability? Just return the raw features from the function?
  private def unpackResponse(input: ForumEvent, response: Response[GetResponse]) =
    if (response.result.found) List((input.personId, RecommendationUtils.getMinHashSignature(getFeatures(response.result), minHasher)))
    else Nil

  def getFeatures(response: GetResponse): Seq[String] = response.source("features").asInstanceOf[List[String]]

}

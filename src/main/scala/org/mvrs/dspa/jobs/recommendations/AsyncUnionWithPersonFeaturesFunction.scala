package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncUnionWithPersonFeaturesFunction(personFeaturesIndex: String, personFeaturesType: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, Set[String]), (Long, Set[String])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, Set[String]),
                           resultFuture: ResultFuture[(Long, Set[String])]): Unit = {
    client.execute {
      get(input._1.toString) from personFeaturesIndex / personFeaturesType
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, Set[String]), response: Response[GetResponse]) =
    if (response.result.found)
      List(
        (
          input._1,
          input._2 ++ getFeatures(response.result)
        )
      )
    else Nil

  def getFeatures(response: GetResponse): Seq[String] = response.source("features").asInstanceOf[List[String]]

}

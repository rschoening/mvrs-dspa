package org.mvrs.dspa.jobs.recommendations

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.MinHashSignature
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mvrs.dspa.io.{AsyncElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class AsyncExcludeKnownPersons(knownPersonsIndex: String, knownPersonsType: String, nodes: ElasticSearchNode*)
  extends AsyncElasticSearchFunction[(Long, MinHashSignature, Set[Long]), (Long, MinHashSignature, Set[Long])](nodes: _*) {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def asyncInvoke(client: ElasticClient,
                           input: (Long, MinHashSignature, Set[Long]),
                           resultFuture: ResultFuture[(Long, MinHashSignature, Set[Long])]): Unit = {
    import scala.collection.JavaConverters._

    client.execute {
      get(input._1.toString) from knownPersonsIndex / knownPersonsType
    }.onComplete {
      case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }
  }

  private def unpackResponse(input: (Long, MinHashSignature, Set[Long]), response: Response[GetResponse]) =
    if (response.result.found)
      List(
        (
          input._1,
          input._2,
          input._3 -- unpackKnownUsers(response) // subtract the known users from the set
        )
      )
    else Nil

  private def unpackKnownUsers(response: Response[GetResponse]) =
    response.result.sourceAsMap("knownUsers")
      .asInstanceOf[List[Int]]
      .map(_.toLong)
}

package org.mvrs.dspa.recommendations

import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.events.ForumEvent
import org.mvrs.dspa.functions.AsyncElasticSearchFunction
import org.mvrs.dspa.{streams, utils}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Recommendations extends App {
  val elasticSearchUri = "http://localhost:9200"

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(3)

  val consumerGroup = "activePostStatistics"
  val speedupFactor = 0 // 0 --> read as fast as can
  val randomDelay = 0 // event time

  val commentsStream = streams.comments(consumerGroup, speedupFactor, randomDelay)
  val postsStream = streams.posts(consumerGroup, speedupFactor, randomDelay)
  val likesStream = streams.likes(consumerGroup, speedupFactor, randomDelay)

  val minHasher = utils.createMinHasher()

  println("s")

  val eventStream =
    commentsStream
      .map(_.asInstanceOf[ForumEvent])
      .union(
        postsStream.map(_.asInstanceOf[ForumEvent]),
        likesStream.map(_.asInstanceOf[ForumEvent]))
      .keyBy(_.personId)

  // FIRST trial: get stored features for person, calculate minhash and buckets, search (asynchronously) for other users in same buckets
  // later: get tags for all posts the user interacted with
  // - either: store post -> tags in elastic
  // - or: broadcast post events to all recommendation operators -> they maintain this mapping


  val minHashes: SingleOutputStreamOperator[(Long, MinHashSignature)] = AsyncDataStream.unorderedWait(
    eventStream.javaStream,
    new AsyncMinHashLookup(elasticSearchUri, minHasher),
    1000L, TimeUnit.MILLISECONDS,
    10)

  // TODO exclude known users
  val recommendations: SingleOutputStreamOperator[(Long, List[(Long, Double)])] = AsyncDataStream.unorderedWait(
    minHashes,
    new AsyncSimilarUsersLookup(elasticSearchUri, minHasher),
    1000L, TimeUnit.MILLISECONDS,
    1)

  recommendations.print()

  env.execute("recommendations")


  def getMinHashSignature(features: Seq[String], minHasher: MinHasher32): MinHashSignature = minHasher.combineAll(features.map(minHasher.init))

  private def getTopN(minHashSignature: MinHashSignature, candidates: List[(Long, MinHashSignature)], n: Int) = {
    candidates.map(candidate => (candidate._1, minHasher.similarity(minHashSignature, candidate._2)))
      .sortBy(-1 * _._2)
      .take(n)
  }

  private def unpackHit(hit: SearchHit) = {
    val list = hit.sourceAsMap("users").asInstanceOf[List[Map[String, (Long, String)]]]

    val result = hit.sourceAsMap("users").asInstanceOf[List[Map[String, Any]]]
    result.map(t => (t("uid").asInstanceOf[Int].toLong, utils.decodeMinHashSignature(t("minhash").asInstanceOf[String])))
  }

  class AsyncSimilarUsersLookup(elasticSearchUri: String, minHasher: MinHasher32) extends AsyncElasticSearchFunction[(Long, MinHashSignature), (Long, List[(Long, Double)])](elasticSearchUri) {

    import com.sksamuel.elastic4s.http.ElasticDsl._

    override def asyncInvoke(client: ElasticClient, input: (Long, MinHashSignature), resultFuture: ResultFuture[(Long, List[(Long, Double)])]): Unit = {
      import scala.collection.JavaConverters._

      val buckets = minHasher.buckets(input._2)
      val personId = input._1

      // println(s"buckets for ${input._1}: $buckets")
      client.execute {
        search("recommendation_lsh_buckets").query {
          termsQuery("_id", buckets.map(_.toString)) // TODO exclude personId from results
        }
      }.onComplete {
        case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }

    private def unpackResponse(input: (Long, MinHashSignature), response: Response[SearchResponse]): List[(Long, List[(Long, Double)])] = {
      val buckets = response.result.hits.hits.map(hit => unpackHit(hit)).toList

      val personId = input._1
      val candidates = buckets.flatMap(bucketResult => bucketResult.filter(user => user._1 != personId))

      // TODO exclude known users
      // TODO exclude inactive users

      List((personId, getTopN(input._2, candidates, 5)))
    }
  }

  class AsyncMinHashLookup(elasticSearchUri: String, minHasher: MinHasher32) extends AsyncElasticSearchFunction[ForumEvent, (Long, MinHashSignature)](elasticSearchUri) {

    import com.sksamuel.elastic4s.http.ElasticDsl._

    override def asyncInvoke(client: ElasticClient, input: ForumEvent, resultFuture: ResultFuture[(Long, MinHashSignature)]): Unit = {
      client.execute {
        get(input.personId.toString).from("recommendation_features" / "personFeatures")
      }.onComplete {
        case Success(response) => resultFuture.complete(unpackResponse(input, response).asJava)
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }

    private def unpackResponse(input: ForumEvent, response: Response[GetResponse]) =
      if (response.result.found) List((input.personId, getMinHashSignature(getFeatures(response.result), minHasher)))
      else Nil

    def getFeatures(response: GetResponse): Seq[String] =
      response.source("features").asInstanceOf[List[String]]

  }


}




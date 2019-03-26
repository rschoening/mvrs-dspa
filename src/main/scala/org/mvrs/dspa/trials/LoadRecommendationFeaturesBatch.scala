package org.mvrs.dspa.trials

import java.nio.file.Paths
import java.util.Base64

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.utils

object LoadRecommendationFeaturesBatch extends App {
  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val rootPath = raw"C:\\data\\dspa\\project\\1k-users-sorted\\tables"
  val hasInterestCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString
  val studyAtCsv = Paths.get(rootPath, "person_studyAt_organisation.csv").toString

  val elasticSearchUri = "http://localhost:9200"

  val bucketsIndex = "recommendation_lsh_buckets"

  val featuresIndex = "recommendation_features"
  val personFeaturesTypeName = "personFeatures"
  val forumFeaturesTypeName = "forumFeatures"
  val bucketTypeName = "buckets"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    dropIndex(client, featuresIndex)
    dropIndex(client, bucketsIndex)
    createFeaturesIndex(client, featuresIndex, personFeaturesTypeName)
    createFeaturesIndex(client, featuresIndex, forumFeaturesTypeName)
    createBucketIndex(client, bucketsIndex, bucketTypeName)
  }
  finally {
    client.close()
  }

  val personInterests = utils.readCsv[(Long, Long)](hasInterestCsv).map(toFeature(_, "I"))
  val personWork = utils.readCsv[(Long, Long)](worksAtCsv).map(toFeature(_, "W"))
  val personStudy = utils.readCsv[(Long, Long)](studyAtCsv).map(toFeature(_, "S"))

  // TODO do example for hierarchy (place, tag structure) --> flatten over all levels
  val personFeatures: DataSet[(Long, Seq[String])] =
    personInterests
      .union(personWork)
      .union(personStudy)
      .groupBy(_._1)
      .reduceGroup(ts => (ts.collectFirst { case t => t._1 }.get,
        ts.map(_._2).toSeq.sorted))

  val minHasher: MinHasher32 = createMinHasher()

  val personMinHashes: DataSet[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, minHasher.combineAll(t._2.map(minHasher.init))))

  val personMinHashBuckets: DataSet[(Long, MinHashSignature, List[Long])] =
    personMinHashes.map(t => (
      t._1, // person id
      t._2, // minhash
      minHasher.buckets(t._2)))

  val buckets: DataSet[(Long, Seq[(Long, MinHashSignature)])] =
    personMinHashBuckets
      .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, (t._1, t._2))))
      .groupBy(_._1) // group by bucket id
      .reduceGroup(ts => (ts.collectFirst { case (bucket, _) => bucket }.get, ts.map(_._2).toSeq.sortBy(_._1))) // TODO revise: why are some sets empty
      .filter(_._2.nonEmpty)

  buckets.first(10).print()

  personFeatures.output(new FeaturesOutputFormat(elasticSearchUri, featuresIndex, personFeaturesTypeName))
  buckets.output(new BucketsOutputFormat(elasticSearchUri, bucketsIndex, bucketTypeName))

  env.execute("import features and buckets")

  // trial read (will eventually get users/signatures based on list of bucket ids, union the users, compare with current (event) user, order on similarity, take(n))
  println(readUsersForBucketTrial(-8940471233175404919L))

  private def readUsersForBucketTrial(bucketId: Long): List[(Long, MinHashSignature)] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val client2 = ElasticClient(ElasticProperties("http://localhost:9200"))

    val result: GetResponse = client2.execute {
      get(bucketId.toString).from(bucketsIndex / bucketTypeName)
    }.await.result
    client2.close()

    // TODO define/use HitReader type class
    val users = result.source("users").asInstanceOf[List[Map[String, Any]]]

    users.map(m => (m("uid").toString.toLong, decodeMinHashSignature(m("minhash").asInstanceOf[String])))
  }

  private def decodeMinHashSignature(base64: String) = MinHashSignature(Base64.getDecoder.decode(base64))

  private def createMinHasher(numHashes: Int = 100, targetThreshold: Double = 0.2): MinHasher32 =
    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) = (input._1, s"$prefix${input._2}")

  private def createBucketIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          nestedField("users").fields(
            longField("uid"),
            binaryField("minhash")),
          dateField("lastUpdate")
        )
      )
    }.await

  }

  private def createFeaturesIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          textField("features"),
          dateField("lastUpdate")
        )
      )
    }.await

  }

  private def dropIndex(client: ElasticClient, indexName: String) = {
    // we must import the dsl
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      deleteIndex(indexName)
    }.await
  }

  private class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
    extends ElasticSearchOutputFormat[(Long, Seq[(Long, MinHashSignature)])](uri) {

    import com.sksamuel.elastic4s.http.ElasticDsl._

    //    import scala.concurrent.ExecutionContext.Implicits.global

    override def process(record: (Long, Seq[(Long, MinHashSignature)]), client: ElasticClient): Unit = {
      // NOTE: connections are "unexpectedly closed" when using onComplete on the future - need to await
      client.execute {
        indexInto(indexName / typeName)
          .withId(record._1.toString)
          .fields(
            "users" -> record._2.map(t => Map(
              "uid" -> t._1,
              "minhash" -> Base64.getEncoder.encodeToString(t._2.bytes))),
            "lastUpdate" -> System.currentTimeMillis())
      }.await
      //        .onComplete {
      //        case Failure(exception) => println(exception)
      //        case _ =>
      //      }
    }
  }

  private class FeaturesOutputFormat(uri: String, indexName: String, typeName: String)
    extends ElasticSearchOutputFormat[(Long, Seq[String])](uri) {

    import com.sksamuel.elastic4s.http.ElasticDsl._

    //    import scala.concurrent.ExecutionContext.Implicits.global

    override def process(record: (Long, Seq[String]), client: ElasticClient): Unit = {
      client.execute {
        indexInto(indexName / typeName)
          .withId(record._1.toString)
          .fields(
            "features" -> record._2,
            "lastUpdate" -> System.currentTimeMillis())
      }.await

      //          .onComplete {
      //          case Failure(exception) => println(exception)
      //          case _ =>
      //        }
    }
  }

}






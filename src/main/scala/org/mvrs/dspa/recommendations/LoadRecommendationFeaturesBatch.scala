package org.mvrs.dspa.recommendations

import java.nio.file.Paths

import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala._
import org.mvrs.dspa.utils

object LoadRecommendationFeaturesBatch extends App {
  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  // TODO determine how to manage settings

  val rootPath = raw"C:\\data\\dspa\\project\\1k-users-sorted\\tables"
  val hasInterestCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString
  val studyAtCsv = Paths.get(rootPath, "person_studyAt_organisation.csv").toString
  val knownPersonsCsv = Paths.get(rootPath, "person_knows_person.csv").toString

  val elasticSearchUri = "http://localhost:9200"

  val bucketsIndex = "recommendation_lsh_buckets"
  val featuresIndex = "recommendation_features"
  val knownPersonsIndex = "recommendation_known_persons"

  val personFeaturesTypeName = "personFeatures"
  val forumFeaturesTypeName = "forumFeatures"
  val bucketTypeName = "buckets"
  val knownPersonsTypeName = "recommendation_known_persons_type"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, featuresIndex)
    utils.dropIndex(client, bucketsIndex)
    utils.dropIndex(client, knownPersonsIndex)
    createFeaturesIndex(client, featuresIndex, personFeaturesTypeName)
    createFeaturesIndex(client, featuresIndex, forumFeaturesTypeName)
    createBucketIndex(client, bucketsIndex, bucketTypeName)
    createKnownPersonsIndex(client, knownPersonsIndex, knownPersonsTypeName)
  }
  finally {
    client.close()
  }

  val personInterests = utils.readCsv[(Long, Long)](hasInterestCsv).map(toFeature(_, "I"))
  val personWork = utils.readCsv[(Long, Long)](worksAtCsv).map(toFeature(_, "W"))
  val personStudy = utils.readCsv[(Long, Long)](studyAtCsv).map(toFeature(_, "S"))

  // TODO load known users

  // TODO do example for hierarchy (place, tag structure) --> flatten over all levels
  val personFeatures: DataSet[(Long, List[String])] =
    personInterests
      .union(personWork)
      .union(personStudy)
      .groupBy(_._1)
      .reduceGroup(sortedValues[String] _)

  val minHasher: MinHasher32 = utils.createMinHasher()

  val personMinHashes: DataSet[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, minHasher.combineAll(t._2.map(minHasher.init))))

  val personMinHashBuckets: DataSet[(Long, MinHashSignature, List[Long])] =
    personMinHashes.map(t => (
      t._1, // person id
      t._2, // minhash
      minHasher.buckets(t._2)))

  // TODO write minhashes to separate index
  val buckets: DataSet[(Long, Seq[(Long, MinHashSignature)])] = personMinHashBuckets
    .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, (t._1, t._2))))
    .groupBy(_._1) // group by bucket id
    .reduceGroup(_.foldLeft[(Long, List[(Long, MinHashSignature)])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2)))

  val knownPersons = utils.readCsv[(Long, Long)](knownPersonsCsv)
    .groupBy(_._1)
    .reduceGroup(sortedValues[Long] _)


  personFeatures.output(new FeaturesOutputFormat(elasticSearchUri, featuresIndex, personFeaturesTypeName))
  buckets.output(new BucketsOutputFormat(elasticSearchUri, bucketsIndex, bucketTypeName))
  knownPersons.output(new KnownUsersOutputFormat(elasticSearchUri, bucketsIndex, bucketTypeName))

  println("users: " + buckets.flatMap(b => b._2).map(_._1).distinct().count())
  println("minhashes: " + buckets.flatMap(b => b._2).map(_._2).distinct().count())
  println("buckets: " + buckets.count())

  env.execute("import features and buckets")

  // trial read (will eventually get users/signatures based on list of bucket ids, union the users, compare with current (event) user, order on similarity, take(n))
  println(readUsersForBucketTrial(-8940471233175404919L))

  private def sortedValues[V: Ordering](x: Iterator[(Long, V)]) = {
    val t = x.foldLeft[(Long, List[V])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2))
    (t._1, t._2.sorted)
  }

  private def readUsersForBucketTrial(bucketId: Long): List[(Long, MinHashSignature)] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val client2 = ElasticClient(ElasticProperties("http://localhost:9200"))

    val result: GetResponse = client2.execute {
      get(bucketId.toString).from(bucketsIndex / bucketTypeName)
    }.await.result
    client2.close()

    // TODO define/use HitReader type class
    val users = result.source("users").asInstanceOf[List[Map[String, Any]]]

    users.map(m => (m("uid").toString.toLong, utils.decodeMinHashSignature(m("minhash").asInstanceOf[String])))
  }

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) = (input._1, s"$prefix${input._2}")

  private def createKnownPersonsIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("knownUsers"),
          dateField("lastUpdate")
        )
      )
    }.await

  }


  private def createBucketIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

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

    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          textField("features"),
          dateField("lastUpdate")
        )
      )
    }.await

  }


}





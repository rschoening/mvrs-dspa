package org.mvrs.dspa.jobs.recommendations.staticdata

import java.nio.file.Paths

import com.sksamuel.elastic4s.http.ElasticClient
import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.io.{ElasticSearchNode, ElasticSearchUtils}
import org.mvrs.dspa.jobs.recommendations.RecommendationUtils
import org.mvrs.dspa.{Settings, utils}

object LoadStaticDataJob extends App {
  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  // TODO determine how to manage settings

  val rootPath = Settings.tablesDirectory
  val hasInterestCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString
  val studyAtCsv = Paths.get(rootPath, "person_studyAt_organisation.csv").toString
  val knownPersonsCsv = Paths.get(rootPath, "person_knows_person.csv").toString

  val elasticSearchNode = ElasticSearchNode("localhost")

  val bucketsIndex = "recommendation_lsh_buckets"
  val personFeaturesIndex = "recommendation_person_features"
  val knownPersonsIndex = "recommendation_known_persons"
  val personMinHashIndex = "recommendation_person_minhash"

  val typeSuffix = "_type"
  val personFeaturesTypeName = personFeaturesIndex + typeSuffix
  val bucketTypeName = bucketsIndex + typeSuffix
  val knownPersonsTypeName = knownPersonsIndex + typeSuffix
  val personMinHashIndexType = personMinHashIndex + typeSuffix

  val client = ElasticSearchUtils.createClient(elasticSearchNode)
  try {
    ElasticSearchUtils.dropIndex(client, personFeaturesIndex)
    ElasticSearchUtils.dropIndex(client, bucketsIndex)
    ElasticSearchUtils.dropIndex(client, knownPersonsIndex)
    ElasticSearchUtils.dropIndex(client, personMinHashIndex)

    createFeaturesIndex(client, personFeaturesIndex, personFeaturesTypeName)
    createKnownPersonsIndex(client, knownPersonsIndex, knownPersonsTypeName)
    createMinHashIndex(client, personMinHashIndex, personMinHashIndexType)
    createBucketIndex(client, bucketsIndex, bucketTypeName)
  }
  finally {
    client.close()
  }

  val personInterests = utils.readCsv[(Long, Long)](hasInterestCsv).map(toFeature(_, "I"))
  val personWork = utils.readCsv[(Long, Long)](worksAtCsv).map(toFeature(_, "W"))
  val personStudy = utils.readCsv[(Long, Long)](studyAtCsv).map(toFeature(_, "S"))

  // TODO do example for hierarchy (place, tag structure) --> flatten over all levels
  val personFeatures: DataSet[(Long, List[String])] =
    personInterests
      .union(personWork)
      .union(personStudy)
      .groupBy(_._1)
      .reduceGroup(sortedValues[String] _)

  val minHasher: MinHasher32 = RecommendationUtils.createMinHasher()

  val personMinHashes: DataSet[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, minHasher.combineAll(t._2.map(minHasher.init))))

  val personMinHashBuckets: DataSet[(Long, MinHashSignature, List[Long])] =
    personMinHashes.map(t => (
      t._1, // person id
      t._2, // minhash
      minHasher.buckets(t._2)))

  val buckets: DataSet[(Long, List[Long])] = personMinHashBuckets
    .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, t._1)))
    .groupBy(_._1) // group by bucket id
    .reduceGroup(_.foldLeft[(Long, List[Long])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2)))

  val knownPersons = utils.readCsv[(Long, Long)](knownPersonsCsv)
    .groupBy(_._1)
    .reduceGroup(sortedValues[Long] _)

  personFeatures.output(new FeaturesOutputFormat(personFeaturesIndex, personFeaturesTypeName, elasticSearchNode))
  personMinHashes.output(new MinHashOutputFormat(personMinHashIndex, personMinHashIndexType, elasticSearchNode))
  buckets.output(new BucketsOutputFormat(bucketsIndex, bucketTypeName, elasticSearchNode))
  knownPersons.output(new KnownUsersOutputFormat(knownPersonsIndex, knownPersonsTypeName, elasticSearchNode))

  env.execute("import static data for recommendations")

  private def sortedValues[V: Ordering](x: Iterator[(Long, V)]): (Long, List[V]) = {
    val t: (Long, List[V]) = x.foldLeft[(Long, List[V])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2))
    (t._1, t._2.sorted)
  }

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) = (input._1, s"$prefix${input._2}")

  private def createKnownPersonsIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("knownUsers").index(false),
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
          longField("uid").index(false),
          dateField("lastUpdate")
        )
      )
    }.await

  }

  private def createMinHashIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          binaryField("minhash").index(false),
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
          textField("features").index(false),
          dateField("lastUpdate")
        )
      )
    }.await

  }


}

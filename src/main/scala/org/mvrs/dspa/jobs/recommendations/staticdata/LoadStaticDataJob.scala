package org.mvrs.dspa.jobs.recommendations.staticdata

import java.nio.file.Paths

import com.twitter.algebird.{MinHashSignature, MinHasher32}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.mvrs.dspa.io.ElasticSearchNode
import org.mvrs.dspa.jobs.recommendations.{FeaturePrefix, RecommendationUtils}
import org.mvrs.dspa.{Settings, utils}

object LoadStaticDataJob extends App {
  val localWithUI = false // TODO arg

  // TODO determine how to manage settings
  val rootPath = Settings.tablesDirectory
  val hasInterestInTagCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val forumTagsCsv = Paths.get(rootPath, "forum_hasTag_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString
  val studyAtCsv = Paths.get(rootPath, "person_studyAt_organisation.csv").toString
  val knownPersonsCsv = Paths.get(rootPath, "person_knows_person.csv").toString

  val elasticSearchNode = ElasticSearchNode("localhost")

  val bucketsIndexName = "recommendation_lsh_buckets"
  val personFeaturesIndexName = "recommendation_person_features"
  val forumFeaturesIndexName = "recommendation_forum_features"
  val knownPersonsIndexName = "recommendation_known_persons"
  val personMinHashIndexName = "recommendation_person_minhash"

  val typeSuffix = "_type"
  val personFeaturesTypeName = personFeaturesIndexName + typeSuffix
  val forumFeaturesTypeName = forumFeaturesIndexName + typeSuffix
  val bucketTypeName = bucketsIndexName + typeSuffix
  val knownPersonsTypeName = knownPersonsIndexName + typeSuffix
  val personMinHashIndexType = personMinHashIndexName + typeSuffix

  val personFeaturesIndex = new FeaturesIndex(personFeaturesIndexName, personFeaturesTypeName, elasticSearchNode)
  val forumFeaturesIndex = new FeaturesIndex(forumFeaturesIndexName, forumFeaturesTypeName, elasticSearchNode)
  val personMinHashIndex = new PersonMinHashIndex(personMinHashIndexName, personMinHashIndexType, elasticSearchNode)
  val knownPersonsIndex = new KnownUsersIndex(knownPersonsIndexName, knownPersonsTypeName, elasticSearchNode)
  val personBucketsIndex = new PersonBucketsIndex(bucketsIndexName, bucketTypeName, elasticSearchNode)

  personFeaturesIndex.create()
  forumFeaturesIndex.create()
  personMinHashIndex.create()
  knownPersonsIndex.create()
  personBucketsIndex.create()

  implicit val env: ExecutionEnvironment = utils.createBatchExecutionEnvironment(localWithUI)
  env.setParallelism(4)

  val personTagInterests = utils.readCsv[(Long, Long)](hasInterestInTagCsv).map(toFeature(_, FeaturePrefix.Tag))
  val personWork = utils.readCsv[(Long, Long)](worksAtCsv).map(toFeature(_, FeaturePrefix.Work))
  val personStudy = utils.readCsv[(Long, Long)](studyAtCsv).map(toFeature(_, FeaturePrefix.Study))
  val forumTags = utils.readCsv[(Long, Long)](forumTagsCsv).map(toFeature(_, FeaturePrefix.Tag))

  // TODO do example for hierarchy (place, tag structure) --> flatten over all levels
  val personFeatures: DataSet[(Long, List[String])] =
    personTagInterests
      .union(personWork)
      .union(personStudy)
      .groupBy(_._1)
      .reduceGroup(sortedValues[String] _)

  val forumFeatures: DataSet[(Long, List[String])] =
    forumTags
      .groupBy(_._1)
      .reduceGroup(sortedValues[String] _)

  val minHasher: MinHasher32 = RecommendationUtils.createMinHasher()

  val personMinHashes: DataSet[(Long, MinHashSignature)] =
    personFeatures.map(t => (t._1, RecommendationUtils.getMinHashSignature(t._2, minHasher)))

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

  personFeatures.output(personFeaturesIndex.createUpsertFormat())
  forumFeatures.output(forumFeaturesIndex.createUpsertFormat())
  personMinHashes.output(personMinHashIndex.createUpsertFormat())
  knownPersons.output(knownPersonsIndex.createUpsertFormat())
  buckets.output(personBucketsIndex.createUpsertFormat())

  env.execute("import static data for recommendations")

  private def sortedValues[V: Ordering](x: Iterator[(Long, V)]): (Long, List[V]) = {
    val t: (Long, List[V]) = x.foldLeft[(Long, List[V])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2))
    (t._1, t._2.sorted)
  }

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) =
    (input._1, RecommendationUtils.toFeature(input._2, prefix))
}




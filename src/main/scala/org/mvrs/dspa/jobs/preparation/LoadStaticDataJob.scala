package org.mvrs.dspa.jobs.preparation

import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.scala.{DataSet, _}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkBatchJob
import org.mvrs.dspa.jobs.recommendations.{FeaturePrefix, RecommendationUtils}
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Batch job for loading static tables into ElasticSearch.
  */
object LoadStaticDataJob extends FlinkBatchJob {
  def execute(): JobExecutionResult = {

    // read settings
    val directory = Settings.config.getString("data.tables-directory")
    val hasInterestInTagCsv = path(directory, "person_hasInterest_tag.csv")
    val forumTagsCsv = path(directory, "forum_hasTag_tag.csv")
    val worksAtCsv = path(directory, "person_workAt_organisation.csv")
    val studyAtCsv = path(directory, "person_studyAt_organisation.csv")
    val knownPersonsCsv = path(directory, "person_knows_person.csv")
    val forumsCsv = path(directory, "forum.csv")

    // (re)create ElasticSearch indexes
    ElasticSearchIndexes.personFeatures.create()
    ElasticSearchIndexes.forumFeatures.create()
    ElasticSearchIndexes.personMinHashes.create()
    ElasticSearchIndexes.knownPersons.create()
    ElasticSearchIndexes.lshBuckets.create()

    // read tables containing features that will be used for MinHash/LSH-based recommendations
    val personTagInterests: DataSet[(Long, String)] =
      FlinkUtils
        .readCsv[(Long, Long)](hasInterestInTagCsv)
        .map(toFeature(_, FeaturePrefix.Tag))
        .name("Map -> (id, feature)")

    val personWork: DataSet[(Long, String)] =
      FlinkUtils
        .readCsv[(Long, Long)](worksAtCsv)
        .map(toFeature(_, FeaturePrefix.Work))
        .name("Map -> (id, feature)")

    val personStudy: DataSet[(Long, String)] =
      FlinkUtils
        .readCsv[(Long, Long)](studyAtCsv)
        .map(toFeature(_, FeaturePrefix.Study))
        .name("Map -> (id, feature)")

    val forumTags: DataSet[(Long, String)] =
      FlinkUtils
        .readCsv[(Long, Long)](forumTagsCsv)
        .map(toFeature(_, FeaturePrefix.Tag))
        .name("Map -> (id, feature)")

    // read the forum information
    val forums: DataSet[(Long, String, String)] = FlinkUtils.readCsv[(Long, String, String)](forumsCsv)

    // aggregate per-person features (work place, study place, person tag interests)
    val personFeatures: DataSet[(Long, List[String])] =
      personTagInterests
        .union(personWork).name("Union with person work place")
        .union(personStudy).name("Union with person study place")
        .groupBy(_._1)
        .reduceGroup(sortedValues[String] _).name("Collect sorted tag interests")

    // aggregate per-forum features (forum tags), join with forum information
    val forumFeatures: DataSet[(Long, String, List[String])] =
      forumTags
        .groupBy(_._1)
        .reduceGroup(sortedValues[String] _).name("Collect sorted forum tags")
        .join(forums)
        .where(f => f._1)
        .equalTo(ft => ft._1)
        .map(
          t => (
            t._1._1, // forum id
            t._2._2, // forum title
            t._1._2, // features
          )
        ).name("Map -> (form id, forum title, features)")

    // TODO do example for hierarchy (place, tag structure) --> flatten over all levels

    val personMinHashes: DataSet[(Long, MinHashSignature)] =
      personFeatures.map(t => (t._1, RecommendationUtils.getMinHashSignature(t._2, RecommendationUtils.minHasher)))
        .name("Calculate MinHash signature for person features")

    val personMinHashBuckets: DataSet[(Long, MinHashSignature, List[Long])] =
      personMinHashes.map(t => (
        t._1, // person id
        t._2, // minhash
        RecommendationUtils.minHasher.buckets(t._2)))
        .name("Map -> (person id, MinHash signature, LSH buckets)")

    val buckets: DataSet[(Long, List[Long])] =
      personMinHashBuckets
        .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, t._1)))
        .name("FlatMap -> (bucked  id, person id)")
        .groupBy(_._1) // group by bucket id
        .reduceGroup(_.foldLeft[(Long, List[Long])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2)))
        .name("Collect person ids per bucket (bucket id -> List(person id))")

    val knownPersons: DataSet[(Long, List[Long])] =
      FlinkUtils.readCsv[(Long, Long)](knownPersonsCsv)
        .groupBy(_._1)
        .reduceGroup(sortedValues[Long] _)
        .name("Collect known persons (person id -> List(person id))")

    // write results to ElasticSearch
    personFeatures
      .output(ElasticSearchIndexes.personFeatures.createUpsertFormat())
      .name(ElasticSearchIndexes.personFeatures.indexName)

    forumFeatures
      .output(ElasticSearchIndexes.forumFeatures.createUpsertFormat())
      .name(ElasticSearchIndexes.forumFeatures.indexName)

    personMinHashes
      .output(ElasticSearchIndexes.personMinHashes.createUpsertFormat())
      .name(ElasticSearchIndexes.personMinHashes.indexName)

    knownPersons
      .output(ElasticSearchIndexes.knownPersons.createUpsertFormat())
      .name(ElasticSearchIndexes.knownPersons.indexName)

    buckets
      .output(ElasticSearchIndexes.lshBuckets.createUpsertFormat())
      .name(ElasticSearchIndexes.lshBuckets.indexName)

    FlinkUtils.printBatchExecutionPlan()

    env.execute("Import static data for recommendations")
  }

  private def path(directory: String, fileName: String): String = s"$directory/$fileName"

  private def sortedValues[V: Ordering](x: Iterator[(Long, V)]): (Long, List[V]) = {
    val t: (Long, List[V]) = x.foldLeft[(Long, List[V])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2))
    (t._1, t._2.sorted)
  }

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) =
    (input._1, RecommendationUtils.toFeature(input._2, prefix))
}

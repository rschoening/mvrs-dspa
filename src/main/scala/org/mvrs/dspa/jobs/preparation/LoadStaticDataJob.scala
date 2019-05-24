package org.mvrs.dspa.jobs.preparation

import com.twitter.algebird.MinHashSignature
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.scala.{DataSet, _}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.db.ElasticSearchIndexes
import org.mvrs.dspa.jobs.FlinkBatchJob
import org.mvrs.dspa.jobs.recommendations.{FeaturePrefix, RecommendationUtils}
import org.mvrs.dspa.utils.FlinkUtils

object LoadStaticDataJob extends FlinkBatchJob {
  def execute(): JobExecutionResult = {

    val directory = Settings.config.getString("data.tables-directory")
    val hasInterestInTagCsv = path(directory, "person_hasInterest_tag.csv")
    val forumTagsCsv = path(directory, "forum_hasTag_tag.csv")
    val worksAtCsv = path(directory, "person_workAt_organisation.csv")
    val studyAtCsv = path(directory, "person_studyAt_organisation.csv")
    val knownPersonsCsv = path(directory, "person_knows_person.csv")
    val forumsCsv = path(directory, "forum.csv")

    ElasticSearchIndexes.personFeatures.create()
    ElasticSearchIndexes.forumFeatures.create()
    ElasticSearchIndexes.personMinHashes.create()
    ElasticSearchIndexes.knownPersons.create()
    ElasticSearchIndexes.lshBuckets.create()

    val personTagInterests = FlinkUtils.readCsv[(Long, Long)](hasInterestInTagCsv).map(toFeature(_, FeaturePrefix.Tag))
    val personWork = FlinkUtils.readCsv[(Long, Long)](worksAtCsv).map(toFeature(_, FeaturePrefix.Work))
    val personStudy = FlinkUtils.readCsv[(Long, Long)](studyAtCsv).map(toFeature(_, FeaturePrefix.Study))
    val forumTags = FlinkUtils.readCsv[(Long, Long)](forumTagsCsv).map(toFeature(_, FeaturePrefix.Tag))

    val forums = FlinkUtils.readCsv[(Long, String, String)](forumsCsv)

    // TODO do example for hierarchy (place, tag structure) --> flatten over all levels
    val personFeatures: DataSet[(Long, List[String])] =
      personTagInterests
        .union(personWork)
        .union(personStudy)
        .groupBy(_._1)
        .reduceGroup(sortedValues[String] _)

    val forumFeatures: DataSet[(Long, String, List[String])] =
      forumTags
        .groupBy(_._1)
        .reduceGroup(sortedValues[String] _)
        .join(forums)
        .where(f => f._1)
        .equalTo(ft => ft._1)
        .map(
          t => (
            t._1._1, // forum id
            t._2._2, // forum title
            t._1._2, // features
          )
        )

    val personMinHashes: DataSet[(Long, MinHashSignature)] =
      personFeatures.map(t => (t._1, RecommendationUtils.getMinHashSignature(t._2, RecommendationUtils.minHasher)))

    val personMinHashBuckets: DataSet[(Long, MinHashSignature, List[Long])] =
      personMinHashes.map(t => (
        t._1, // person id
        t._2, // minhash
        RecommendationUtils.minHasher.buckets(t._2)))

    val buckets: DataSet[(Long, List[Long])] =
      personMinHashBuckets
        .flatMap((t: (Long, MinHashSignature, List[Long])) => t._3.map(bucket => (bucket, t._1)))
        .groupBy(_._1) // group by bucket id
        .reduceGroup(_.foldLeft[(Long, List[Long])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2)))

    val knownPersons =
      FlinkUtils.readCsv[(Long, Long)](knownPersonsCsv)
        .groupBy(_._1)
        .reduceGroup(sortedValues[Long] _)

    personFeatures.output(ElasticSearchIndexes.personFeatures.createUpsertFormat())
    forumFeatures.output(ElasticSearchIndexes.forumFeatures.createUpsertFormat())
    personMinHashes.output(ElasticSearchIndexes.personMinHashes.createUpsertFormat())
    knownPersons.output(ElasticSearchIndexes.knownPersons.createUpsertFormat())
    buckets.output(ElasticSearchIndexes.lshBuckets.createUpsertFormat())

    env.execute("import static data for recommendations")
  }

  private def path(directory: String, fileName: String): String = s"$directory/$fileName"

  private def sortedValues[V: Ordering](x: Iterator[(Long, V)]): (Long, List[V]) = {
    val t: (Long, List[V]) = x.foldLeft[(Long, List[V])]((0L, Nil))((z, t) => (t._1, t._2 :: z._2))
    (t._1, t._2.sorted)
  }

  private def toFeature(input: (Long, Long), prefix: String): (Long, String) =
    (input._1, RecommendationUtils.toFeature(input._2, prefix))
}

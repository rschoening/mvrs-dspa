package jobs.recommendations

import com.twitter.algebird.MinHasher32
import org.mvrs.dspa.jobs.recommendations.RecommendationUtils
import org.scalatest.{FlatSpec, Matchers}

class RecommendationUtilsTestSuite extends FlatSpec with Matchers {
  "MinHashing" must "provide a valid approximation of the Jaccard similarity" in {
    //noinspection RedundantDefaultArgument
    val minHasher = RecommendationUtils.createMinHasher(numHashes = 100, targetThreshold = 0.2)
    // increase value for numHashes --> reduced approximation error
    // (targetThreshold affects only the number of LSH bands, not the minhash signatures)

    val features = List("A", "B", "C")

    val featuresEqual = List("A", "B", "C")
    val featuresSimilar = List("A", "B", "_")
    val featuresDisjoint = List("X", "Y", "Z")
    val featuresSuperset = features ++ featuresDisjoint
    val featuresSubset = List("A", "C")

    assertResult(1.0)(approximateJaccard(features, featuresEqual, minHasher))
    assertResult(0.0)(approximateJaccard(features, featuresDisjoint, minHasher))

    assertResult(0.51)(approximateJaccard(features, featuresSimilar, minHasher))
    assertResult(0.49)(approximateJaccard(features, featuresSuperset, minHasher))
    assertResult(0.66)(approximateJaccard(features, featuresSubset, minHasher))

    assert(approximationError(features, featuresEqual, minHasher) == 0)
    assert(approximationError(features, featuresDisjoint, minHasher) == 0)

    assert(approximationError(features, featuresSimilar, minHasher) < 0.015)
    assert(approximationError(features, featuresSuperset, minHasher) < 0.015)
    assert(approximationError(features, featuresSubset, minHasher) < 0.015)

    // this would be a case for property-based testing with scalacheck, in case we mistrust twitter and/or Ullman and Rajaraman
  }

  "LSH" must "allow selection of similar sets for a given similarity threshold and accurracy" in {
    //noinspection RedundantDefaultArgument
    val minHasher = RecommendationUtils.createMinHasher(numHashes = 100, targetThreshold = 0.1)

    // some inclusion probability values for given Jaccard similarities
    // - increasing numHashes steepens the probability curve at the target threshold
    println("0.01: " + minHasher.probabilityOfInclusion(0.01)) // 0.004
    println("0.02:" + minHasher.probabilityOfInclusion(0.02)) //  0.017
    println("0.05:" + minHasher.probabilityOfInclusion(0.05)) //  0.102
    println("0.1: " + minHasher.probabilityOfInclusion(0.1)) //   0.350
    println("0.2: " + minHasher.probabilityOfInclusion(0.2)) //   0.827 --> target threshold
    println("0.5: " + minHasher.probabilityOfInclusion(0.5)) //   0.999
    println("0.9: " + minHasher.probabilityOfInclusion(0.9)) //   0.999

    // at target threshold 0.2: 17.3% false negatives with 100 hashes; 10% with 1000 hashes, 3.5% with 10000 hashes
    // With higher values for numHashes, the bucket count gets very large --> high cost for bucket storage/retrieval (and hashing)
    // --> it seems reasonable to use a target threshold that is a bit lower than the minimum similarity to be included in the
    //    recommendation, and use a moderate value for numHashes --> 100 (default)
    // e.g. similarity for inclusion in recommendation >= 0.2 -> targetThreshold = 0.1 and numHashes = 100 => probability for inclusion at 0.2: 99%

    val features = List("A", "B", "C")

    val featuresEqual = List("A", "B", "C")
    val featuresSimilar = List("A", "B", "_")
    val featuresDisjoint1 = List("X", "Y", "Z")
    val featuresDisjoint2 = List("1", "2", "3")
    val featuresDisjoint3 = List(" ", "_", "-")
    val featuresSuperset = features ++ featuresDisjoint1
    val featuresSubset = List("A", "C")

    val searchBuckets = buckets(features, minHasher)

    println(s"buckets for search set: ${searchBuckets.size}") // 50 for default values

    // increase value for numHashes -> more buckets
    // decrease value for targetThreshold -> more buckets (limited by value for numHashes)

    val bucketsById = List(
      ("equal", buckets(featuresEqual, minHasher)),
      ("similar", buckets(featuresSimilar, minHasher)),
      ("superset", buckets(featuresSuperset, minHasher)),
      ("subset", buckets(featuresSubset, minHasher)),
      ("disjoint1", buckets(featuresDisjoint1, minHasher)),
      ("disjoint2", buckets(featuresDisjoint2, minHasher)),
      ("disjoint3", buckets(featuresDisjoint3, minHasher)),
    )

    val idsByBucket: Map[Long, List[(String, Long)]] =
      bucketsById
        .flatMap { case (id, buckets) => buckets.map(bucket => (id, bucket)) }
        .groupBy(_._2) // --> bucket -> (id, bucket)

    println(s"total bucket count: ${idsByBucket.size}") // 292 with default values

    // union all ids that were hashed to the same buckets
    val approximateSelection: Set[String] =
      searchBuckets
        .flatMap(bucket => idsByBucket.getOrElse(bucket, List()).map(_._1))
        .toSet

    println("\nselected sets:")
    println(approximateSelection.map(s => s"- $s").mkString("\n"))

    assert(approximateSelection.forall(!_.startsWith("disjoint"))) // there could be false positives, but we're lucky
  }

  private def approximationError(features: List[String], otherFeatures: List[String], minHasher: MinHasher32): Double =
    math.abs(approximateJaccard(features, otherFeatures, minHasher) - exactJaccard(features.toSet, otherFeatures.toSet))

  private def exactJaccard(set1: Set[String], set2: Set[String]): Double =
    (set1 intersect set2).size / (set1 union set2).size.toDouble

  private def approximateJaccard(features: List[String], otherFeatures: List[String], minHasher: MinHasher32) =
    minHasher.similarity(
      RecommendationUtils.getMinHashSignature(features, minHasher),
      RecommendationUtils.getMinHashSignature(otherFeatures, minHasher))

  private def buckets(features: List[String], minHasher: MinHasher32): List[Long] = {
    minHasher.buckets(RecommendationUtils.getMinHashSignature(features, minHasher))
  }
}

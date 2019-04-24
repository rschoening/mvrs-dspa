package org.mvrs.dspa.db

import org.mvrs.dspa.Settings

object ElasticSearchIndexes {
  private val nodes = Settings.elasticSearchNodes

  val activePostStatistics = new ActivePostStatisticsIndex("mvrs-active-post-statistics", nodes: _*)

  val classification = new ActivityClassificationIndex("mvrs-activity-classification", nodes: _*)
  val clusterMetadata = new ClusterMetadataIndex("mvrs-activity-cluster-metadata", nodes: _*)

  val recommendations = new RecommendationsIndex("mvrs-recommendations", nodes: _*)
  val postFeatures = new PostFeaturesIndex("mvrs-recommendation-post-features", nodes: _*)
  val personFeatures = new FeaturesIndex("mvrs-recommendation-person-features", nodes: _*)
  val forumFeatures = new FeaturesIndex("mvrs-recommendation-forum-features", nodes: _*)
  val personMinHashes = new PersonMinHashIndex("mvrs-recommendation-person-minhash", nodes: _*)
  val knownPersons = new KnownUsersIndex("mvrs-recommendation-known-persons", nodes: _*)
  val lshBuckets = new PersonBucketsIndex("mvrs-recommendation-lsh-buckets", nodes: _*)
}

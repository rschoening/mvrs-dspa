package org.mvrs.dspa.db

import org.mvrs.dspa.Settings

/**
  * Static registry of ElasticSearch indexes used in the project
  */
object ElasticSearchIndexes {
  private val nodes = Settings.elasticSearchNodes

  val activePostStatistics = new ActivePostStatisticsIndex("mvrs-active-post-statistics", nodes: _*)
  val postInfos = new PostInfoIndex("mvrs-active-post-statistics-postinfos", nodes: _*)

  val classification = new ActivityClassificationIndex("mvrs-activity-classification", nodes: _*)
  val clusterMetadata = new ClusterMetadataIndex("mvrs-activity-cluster-metadata", nodes: _*)

  val recommendations = new RecommendationsIndex("mvrs-recommendations", nodes: _*)
  val postFeatures = new PostFeaturesIndex("mvrs-recommendation-post-features", nodes: _*)
  val personFeatures = new PersonFeaturesIndex("mvrs-recommendation-person-features", nodes: _*)
  val forumFeatures = new ForumFeaturesIndex("mvrs-recommendation-forum-features", nodes: _*)
  val personMinHashes = new PersonMinHashIndex("mvrs-recommendation-person-minhash", nodes: _*)
  val knownPersons = new KnownUsersIndex("mvrs-recommendation-known-persons", nodes: _*)
  val lshBuckets = new PersonBucketsIndex("mvrs-recommendation-lsh-buckets", nodes: _*)
  val postMappings = new PostMappingIndex("mvrs-post-for-comment", nodes: _*)
}

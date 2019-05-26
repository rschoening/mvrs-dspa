package org.mvrs.dspa.model

/**
  * Post features relevant in the process of finding similar persons (combination of post tags and forum tags)
  */
case class PostFeatures(postId: Long,
                        personId: Long,
                        timestamp: Long,
                        features: Set[String])
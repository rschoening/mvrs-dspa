package org.mvrs.dspa.model

case class PostFeatures(postId: Long,
                        personId: Long,
                        timestamp: Long,
                        features: Set[String])
package org.mvrs.dspa.jobs.recommendations

case class PostFeatures(postId: Long, 
                        personId: Long,
                        forumId: Long,
                        timestamp: Long,
                        content: String,
                        imageFile: String,
                        features: Set[String])
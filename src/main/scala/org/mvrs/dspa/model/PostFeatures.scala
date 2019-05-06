package org.mvrs.dspa.model

case class PostFeatures(postId: Long,
                        personId: Long,
                        forumId: Long,
                        forumTitle: String,
                        timestamp: Long,
                        content: String,
                        imageFile: String,
                        features: Set[String])



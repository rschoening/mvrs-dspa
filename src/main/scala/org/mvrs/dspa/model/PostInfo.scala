package org.mvrs.dspa.model

case class PostInfo(postId: Long,
                    personId: Long,
                    forumId: Long,
                    forumTitle: String,
                    timestamp: Long,
                    content: String,
                    imageFile: String)

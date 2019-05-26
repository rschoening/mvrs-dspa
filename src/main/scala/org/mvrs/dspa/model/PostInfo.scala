package org.mvrs.dspa.model

/**
  * Core post event information extended with the title of the forum
  *
  */
case class PostInfo(postId: Long,
                    personId: Long,
                    forumId: Long,
                    forumTitle: String,
                    timestamp: Long,
                    content: String,
                    imageFile: String)

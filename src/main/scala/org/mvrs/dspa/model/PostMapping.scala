package org.mvrs.dspa.model

/**
  * Mapping between a comment and a post - used when reconstructing the reply hierarchy
  *
  * @param commentId The comment id
  * @param postId    The post id the comment referes to
  */
case class PostMapping(commentId: Long, postId: Long)

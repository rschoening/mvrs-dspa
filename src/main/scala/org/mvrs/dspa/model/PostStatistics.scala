package org.mvrs.dspa.model

/**
  * The statistics about a post related in a window
  *
  * @param postId            The post id
  * @param time              The time at which the statistics were collected (end time of window)
  * @param commentCount      The number of comments to the post (directly)
  * @param replyCount        The number of replies to comments for the post
  * @param likeCount         The number of likes for the post
  * @param distinctUserCount The number of distinct users involved with the post (commenting, liking, optionally also
  *                          counting the post author)
  * @param newPost           Indicates if the post was created within the time window
  */
case class PostStatistics(postId: Long,
                          time: Long,
                          commentCount: Int,
                          replyCount: Int,
                          likeCount: Int,
                          distinctUserCount: Int,
                          newPost: Boolean)

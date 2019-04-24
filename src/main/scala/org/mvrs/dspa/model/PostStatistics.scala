package org.mvrs.dspa.model

case class PostStatistics(postId: Long,
                          time: Long,
                          commentCount: Int,
                          replyCount: Int,
                          likeCount: Int,
                          distinctUserCount: Int,
                          newPost: Boolean)

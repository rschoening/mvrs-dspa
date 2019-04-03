package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

class BuildReplyTreeProcessFunction(outputTagDroppedReplies: OutputTag[CommentEvent])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]
    with CheckpointedFunction {

  // checkpointed operator state
  private val danglingReplies: mutable.Map[Long, (Long, mutable.Set[CommentEvent])] = mutable.Map[Long, (Long, mutable.Set[CommentEvent])]()
  private val postForComment: mutable.Map[Long, Long] = mutable.Map()
  @transient private var danglingRepliesListState: ListState[Map[Long, Set[CommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, Long]] = _

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    val postId = firstLevelComment.postId
    assert(ctx.getCurrentKey == postId)

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    postForComment.put(firstLevelComment.id, postId)

    out.collect(firstLevelComment)

    // process all replies that were waiting for this comment (recursively)
    if (danglingReplies.contains(firstLevelComment.id)) {

      val resolved = BuildReplyTreeProcessFunction.resolveDanglingReplies(
        danglingReplies(firstLevelComment.id)._2, postId, getDanglingReplies)

      danglingReplies.remove(firstLevelComment.id)
      resolved.foreach(c => danglingReplies.remove(c.id)) // remove resolved replies from operator state
      resolved.foreach(out.collect) // emit the resolved replies
    }

    // register a timer (will call back at watermark for the creationDate)
    ctx.timerService().registerEventTimeTimer(firstLevelComment.creationDate)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    // check among *all* dangling replies
    for {(parentCommentId, (maxTimestamp, replies)) <- danglingReplies} {
      if (maxTimestamp <= timestamp) { // past the watermark
        // check if the post id for the parent comment id is now known
        val postId = postForComment.get(parentCommentId)

        if (postId.isDefined && postId.contains(ctx.getCurrentKey)) {
          // the post id was found -> point all dangling children to it
          val resolved = BuildReplyTreeProcessFunction.resolveDanglingReplies(
            replies, postId.get, getDanglingReplies)

          resolved.foreach(r => postForComment(r.id) = r.postId) // remember comment -> post mapping
          resolved.foreach(r => danglingReplies.remove(r.id)) // remove resolved replies from operator state
          resolved.foreach(out.collect) // emit the resolved replies
        }
        else {
          // post id unknown for referenced comment, and past watermark -> drop replies
          val unresolved = BuildReplyTreeProcessFunction.getDanglingReplies(replies, getDanglingReplies)

          // NOTE with multiple workers, the unresolved replies are emitted for each worker!
          unresolved.foreach(ctx.output(outputTagDroppedReplies, _))

          unresolved.foreach(r => danglingReplies.remove(r.id)) // remove resolved replies from operator state
          danglingReplies.remove(parentCommentId)
        }
      }
    }
  }

  private def getDanglingReplies(event: CommentEvent) =
    danglingReplies.getOrElse(event.id, (Long.MinValue, mutable.Set[CommentEvent]()))._2

  override def processBroadcastElement(reply: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    val postId = postForComment.get(reply.replyToCommentId.get)
    if (postId.isDefined) {
      postForComment(reply.id) = postId.get // remember our new friend
      out.collect(reply.copy(replyToPostId = postId)) // ... and immediately emit the rooted reply
    }
    else rememberDanglingReply(reply) // cache for later evaluation, globally in this operator
  }

  private def rememberDanglingReply(reply: CommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    val newValue =
      danglingReplies
        .get(parentCommentId)
        .map { case (ts, replies) => (math.max(ts, reply.creationDate), replies += reply) }
        .getOrElse((reply.creationDate, mutable.Set(reply)))

    danglingReplies.put(parentCommentId, newValue)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    danglingRepliesListState.clear()
    danglingRepliesListState.add(danglingReplies.map(t => (t._1, t._2._2.toSet)).toMap)

    postForCommentListState.clear()
    postForCommentListState.add(postForComment.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val childRepliesDescriptor = new ListStateDescriptor[Map[Long, Set[CommentEvent]]](
      "child-replies",
      createTypeInformation[Map[Long, Set[CommentEvent]]]
    )
    val postForCommentDescriptor = new ListStateDescriptor[Map[Long, Long]](
      "post-for-comment",
      createTypeInformation[Map[Long, Long]])

    danglingRepliesListState = context.getOperatorStateStore.getListState(childRepliesDescriptor)
    postForCommentListState = context.getOperatorStateStore.getListState(postForCommentDescriptor)

    if (context.isRestored) {
      // merge maps of parent comment id -> dangling replies (with maximum timestamp)
      danglingReplies.clear()
      for (element <- danglingRepliesListState.get().asScala) {
        for {(parentCommentId, replies) <- element} {
          val maxTimestamp = replies.foldLeft(Long.MinValue)((z, r) => math.max(z, r.creationDate))
          val set = mutable.Set(replies.toSeq: _*)
          if (danglingReplies.get(parentCommentId).isEmpty) danglingReplies(parentCommentId) = (maxTimestamp, set)
          else danglingReplies(parentCommentId) = (maxTimestamp, set ++ danglingReplies(parentCommentId)._2)
        }
      }

      // merge comment -> post maps
      postForComment.clear()
      postForCommentListState.get.asScala.foreach(postForComment ++= _)
    }
  }
}

object BuildReplyTreeProcessFunction {
  def resolveDanglingReplies(replies: Iterable[CommentEvent],
                             postId: Long,
                             getChildren: CommentEvent => Iterable[CommentEvent]): Set[CommentEvent] =
    getDanglingReplies(replies, getChildren).map(r => r.copy(replyToPostId = Some(postId)))

  def getDanglingReplies(replies: Iterable[CommentEvent],
                         getChildren: CommentEvent => Iterable[CommentEvent]): Set[CommentEvent] = {
    @tailrec
    def loop(acc: Set[CommentEvent])(replies: Iterable[CommentEvent],
                                     getChildren: CommentEvent => Iterable[CommentEvent]): Set[CommentEvent] = {
      if (replies.isEmpty) acc // base case
      else loop(acc ++ replies)(replies.flatMap(getChildren), getChildren)
    }

    loop(Set())(replies, getChildren)
  }

}

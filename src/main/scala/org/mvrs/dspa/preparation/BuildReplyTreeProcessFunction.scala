package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

class BuildReplyTreeProcessFunction(outputTagDroppedReplies: OutputTag[CommentEvent])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]
    with CheckpointedFunction {

  // checkpointed operator state
  private val danglingReplies: mutable.Map[Long, (Long, mutable.Set[CommentEvent])] = mutable.Map[Long, (Long, mutable.Set[CommentEvent])]()
  private val postForComment: mutable.Map[Long, Long] = mutable.Map()
  private val LOG = LoggerFactory.getLogger(classOf[BuildReplyTreeProcessFunction])
  @transient private var danglingRepliesListState: ListState[Map[Long, Set[CommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, Long]] = _
  private var commentWatermark: Long = Long.MinValue

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    val postId = firstLevelComment.postId
    assert(ctx.getCurrentKey == postId)

    commentWatermark = ctx.currentWatermark()

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    postForComment.put(firstLevelComment.id, postId)

    // process all replies that were waiting for this comment (recursively)
    if (danglingReplies.contains(firstLevelComment.id)) {

      val resolved = resolveDanglingReplies(Set())(
        danglingReplies(firstLevelComment.id)._2,
        postId,
        reply => danglingReplies.getOrElse(reply.id, (Long.MinValue, mutable.Set[CommentEvent]()))._2)

      resolved.foreach(c => danglingReplies.remove(c.id)) // remove resolved replies from operator state

      // emit the resolved replies
      resolved.foreach(out.collect)
    }
  }

  override def processBroadcastElement(reply: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    val parentCommentId = reply.replyToCommentId.get

    val postId = postForComment.get(parentCommentId)

    if (postId.isDefined) out.collect(reply.copy(replyToPostId = postId))
    else {
      // cache for later evaluation, globally in this operator
      val newValue =
        danglingReplies
          .get(parentCommentId)
          .map { case (ts, replies) => (math.max(ts, reply.creationDate), replies += reply) }
          .getOrElse((reply.creationDate, mutable.Set(reply)))

      danglingReplies.put(parentCommentId, newValue)
    }

    // evict all dangling replies older than the watermark
    // println(utils.formatDuration(commentWatermark - ctx.currentWatermark()))
    val watermark = math.max(commentWatermark, ctx.currentWatermark())

    // TODO shouldn't we evict based on watermark of first-level comments?
    val droppedReplies = danglingReplies.filter { case (_, (maxTimestamp, _)) => maxTimestamp <= watermark }

    if (droppedReplies.nonEmpty) {
      // emit dropped replies to side output
      droppedReplies.foreach { case (_, (_, replies)) => replies.foreach(reply => ctx.output(outputTagDroppedReplies, reply)) }

      val beforeDrop = if (LOG.isDebugEnabled()) danglingReplyCount() else 0

      danglingReplies --= droppedReplies.keys

      if (LOG.isDebugEnabled()) {
        val afterDrop = danglingReplyCount()
        LOG.debug(s"[${getRuntimeContext.getIndexOfThisSubtask} / ${getRuntimeContext.getNumberOfParallelSubtasks}] " +
          s"dropped: ${beforeDrop - afterDrop} - remaining: $afterDrop")
      }
    }
  }

  private def danglingReplyCount(): Int = danglingReplies.map { case (_, (_, replies)) => replies.size }.sum

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

  @tailrec
  final def resolveDanglingReplies(acc: Set[CommentEvent])(replies: Iterable[CommentEvent],
                                                           postId: Long,
                                                           getChildren: CommentEvent => mutable.Set[CommentEvent]): Set[CommentEvent] = {
    if (replies.isEmpty) acc // base case
    else {
      val resolvedReplies = replies.map(_.copy(replyToPostId = Some(postId)))

      resolveDanglingReplies(acc ++ resolvedReplies)(resolvedReplies.flatMap(getChildren(_)), postId, getChildren)
    }
  }
}

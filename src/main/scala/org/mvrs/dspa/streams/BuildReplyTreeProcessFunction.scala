package org.mvrs.dspa.streams

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Gauge}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.mvrs.dspa.model.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

class BuildReplyTreeProcessFunction(outputTagDroppedReplies: Option[OutputTag[RawCommentEvent]])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]
    with CheckpointedFunction {

  // checkpointed operator state
  // - since state has to be accessed from the broadcast side also (outside of keyed context), unkeyed state is used.
  //   An alternative would be the use of applyToKeyedState(), however this processes all keyed states in sequence,
  //   which might become too slow if many keys are active
  @transient private lazy val danglingReplies: mutable.Map[Long, mutable.Set[RawCommentEvent]] = mutable.Map[Long, mutable.Set[RawCommentEvent]]()

  // TODO persist to ElasticSearch, use as cache (then no longer in operator state)
  @transient private lazy val postForComment: mutable.Map[Long, Long] = mutable.Map()

  @transient private var danglingRepliesListState: ListState[Map[Long, Set[RawCommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, Long]] = _

  // metrics
  @transient private var resolvedReplyCount: Counter = _
  @transient private var droppedReplyCount: Counter = _
  @transient private var danglingRepliesCount: Gauge[Int] = _
  @transient private var cacheProcessingTime: Gauge[Long] = _
  @transient private var cacheProcessingCount: Counter = _

  @transient private var lastCacheProcessingTime = 0L

  @transient private var currentCommentWatermark: Long = Long.MinValue
  @transient private var currentBroadcastWatermark: Long = Long.MinValue
  @transient private var currentMinimumWatermark: Long = Long.MinValue

  override def open(parameters: Configuration): Unit = {
    // prepare metrics
    val group = getRuntimeContext.getMetricGroup

    resolvedReplyCount = group.counter("resolvedReplyCount")
    droppedReplyCount = group.counter("droppedReplyCount")
    danglingRepliesCount = group.gauge[Int, ScalaGauge[Int]]("danglingRepliesCount", ScalaGauge[Int](() => danglingReplies.size))
    cacheProcessingTime = group.gauge[Long, ScalaGauge[Long]]("cacheProcessingTime", ScalaGauge[Long](() => lastCacheProcessingTime))
    cacheProcessingCount = group.counter("cacheProcessingCount")
  }

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    // there should be no events with timestamps older or equal to the watermark
    assert(firstLevelComment.timestamp > ctx.currentWatermark())
    assert(ctx.timerService().currentWatermark() == ctx.currentWatermark())

    val postId = firstLevelComment.postId

    assert(ctx.getCurrentKey == postId) // must be keyed by post id

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    // NOTE how to deal with cache misses? Must be in this operator, however access to ElasticSearch should be non-blocking
    postForComment.put(firstLevelComment.commentId, postId)

    out.collect(firstLevelComment)

    // process all replies that were waiting for this comment (recursively)
    emitWaitingChildren(firstLevelComment.commentId, out, postId)

    currentCommentWatermark = ctx.currentWatermark()
    val minimumWatermark = math.min(currentCommentWatermark, currentBroadcastWatermark)
    if (minimumWatermark > currentMinimumWatermark) {
      currentMinimumWatermark = minimumWatermark
      processDanglingReplies(currentMinimumWatermark, out, reportDropped(_, (t, r) => ctx.output(t, r)))
    }

    // register a timer (will call back at watermark for the timestamp)
    // NOTE actually the timer should be based on arriving unresolved replies, however the timer can only be registered
    //      in the keyed context of the first-level comments.
    ctx.timerService().registerEventTimeTimer(firstLevelComment.timestamp)
  }

  private def emitWaitingChildren(commentId: Long, out: Collector[CommentEvent], postId: Long) = {
    if (danglingReplies.contains(commentId)) {

      val directChildren = danglingReplies(commentId)

      val resolved = resolveDanglingReplies(directChildren, postId, getWaitingReplies)

      emitResolvedReplies(resolved, out)

      danglingReplies.remove(commentId)
    }
  }

  //  private def processWatermark(newWatermark: Long,
  //                               currentWatermark: Long,
  //                               collector: Collector[CommentEvent],
  //                               sideOutput: (OutputTag[RawCommentEvent], RawCommentEvent) => Unit): Long = {
  //    if (newWatermark > currentWatermark) {
  //      val newMinimum = math.min(newWatermark, currentWatermark)
  //
  //      if (newMinimum > currentMinimumWatermark) {
  //        currentMinimumWatermark = newMinimum
  //        processDanglingReplies(currentMinimumWatermark, collector, reportDropped(_, sideOutput))
  //      }
  //    }
  //
  //    newWatermark
  //  }

  override def onTimer(firstLevelCommentWatermark: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    currentCommentWatermark = firstLevelCommentWatermark
    processDanglingReplies(
      firstLevelCommentWatermark,
      out,
      reportDropped(_, (t, r) => ctx.output(t, r)))
  }


  override def processBroadcastElement(reply: RawCommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {

    // there should be no events with timestamps older or equal to the watermark
    assert(reply.timestamp > ctx.currentWatermark())

    val postId = reply.replyToCommentId.flatMap(postForComment.get)

    if (postId.isDefined) {
      postForComment(reply.commentId) = postId.get // remember our new friend

      out.collect(resolve(reply, postId.get)) // ... and immediately emit the rooted reply

      emitWaitingChildren(reply.commentId, out, postId.get)
    }
    else rememberDanglingReply(reply) // cache for later evaluation, globally in this operator/worker

    // process dangling replies whenever watermark progressed

    currentBroadcastWatermark = ctx.currentWatermark()
    val minimumWatermark = math.min(currentCommentWatermark, currentBroadcastWatermark)

    if (minimumWatermark > currentMinimumWatermark) {
      currentMinimumWatermark = minimumWatermark
      processDanglingReplies(currentMinimumWatermark, out, reportDropped(_, (t, r) => ctx.output(t, r)))
    }
    //    currentBroadcastWatermark = processWatermark(
    //      ctx.currentWatermark(), currentBroadcastWatermark,
    //      out, (t, r) => ctx.output(t, r))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    danglingRepliesListState.clear()
    danglingRepliesListState.add(danglingReplies.map(t => (t._1, t._2.toSet)).toMap)

    postForCommentListState.clear()
    postForCommentListState.add(postForComment.toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val childRepliesDescriptor = new ListStateDescriptor[Map[Long, Set[RawCommentEvent]]](
      "child-replies",
      createTypeInformation[Map[Long, Set[RawCommentEvent]]]
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

          val set = mutable.Set(replies.toSeq: _*)

          if (danglingReplies.get(parentCommentId).isEmpty)
            danglingReplies(parentCommentId) = set
          else
            danglingReplies(parentCommentId) = set ++ danglingReplies(parentCommentId)
        }
      }

      // merge the maps (comment -> post)
      postForComment.clear()
      postForCommentListState.get.asScala.foreach(postForComment ++= _)
    }
  }

  private def reportDropped(reply: RawCommentEvent,
                            output: (OutputTag[RawCommentEvent], RawCommentEvent) => Unit): Unit =
    outputTagDroppedReplies.foreach(output(_, reply))

  private def processDanglingReplies(minimumWatermark: Long,
                                     out: Collector[CommentEvent],
                                     reportDroppedReply: RawCommentEvent => Unit): Unit = {
    val startTime = System.currentTimeMillis()

    // first, let all known parents resolve their children
    for {(parentCommentId, replies) <- danglingReplies} {
      // check if the post id for the parent comment id is now known
      val postId = postForComment.get(parentCommentId)

      postId.foreach(id => {
        // the post id was found -> point all dangling children to it
        val resolved = resolveDanglingReplies(replies, id, getWaitingReplies)

        emitResolvedReplies(resolved, out)

        danglingReplies.remove(parentCommentId)
      })
    }

    for {(parentCommentId, replies) <- danglingReplies} {
      // The parent of a dangling reply can be either a first-level comment or another reply.
      // Only when the watermark of BOTH replies (broadcast) and first-level comments have passed
      // can we be sure that the reply won't ever find it's parent so that it can be dropped
      // -> use the minimum of the watermarks of both input streams

      val lostReplies = replies.filter(_.timestamp <= minimumWatermark)
      if (lostReplies.nonEmpty) {
        val lostRepliesWithChildren = getDanglingReplies(lostReplies, getWaitingReplies)

        lostRepliesWithChildren.foreach(lostReply => {
          danglingReplies.remove(lostReply.commentId) // remove resolved replies from operator state

          reportDroppedReply(lostReply)
          droppedReplyCount.inc()
        })

        replies --= lostReplies // replies is *mutable* set

        if (replies.isEmpty) danglingReplies.remove(parentCommentId) // no remaining waiting replies
      }
    }

    val duration = System.currentTimeMillis() - startTime

    // metrics
    lastCacheProcessingTime = duration
    cacheProcessingCount.inc()
  }

  private def emitResolvedReplies(resolved: Iterable[CommentEvent], out: Collector[CommentEvent]): Unit = {
    val count = resolved.size
    require(count > 0, "empty replies iterable")

    resolved.foreach(resolvedReply => {
      postForComment(resolvedReply.commentId) = resolvedReply.postId // remember comment -> post mapping
      danglingReplies.remove(resolvedReply.commentId) // remove resolved replies from operator state
      out.collect(resolvedReply) // emit the resolved replies
    })

    // metrics
    resolvedReplyCount.inc(count)
  }

  private def getWaitingReplies(event: RawCommentEvent): mutable.Set[RawCommentEvent] =
    danglingReplies.getOrElse(event.commentId, mutable.Set[RawCommentEvent]())

  private def rememberDanglingReply(reply: RawCommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    danglingReplies.get(parentCommentId) match {
      case Some(replies) => replies += reply // mutable, updated in place
      case None => danglingReplies.put(parentCommentId, mutable.Set(reply))
    }
  }
}

object BuildReplyTreeProcessFunction {
  def resolveDanglingReplies(replies: Iterable[RawCommentEvent],
                             postId: Long,
                             getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[CommentEvent] =
    getDanglingReplies(replies, getChildren).map(r => resolve(r, postId))

  def getDanglingReplies(replies: Iterable[RawCommentEvent],
                         getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[RawCommentEvent] = {
    @tailrec
    def loop(acc: Set[RawCommentEvent])
            (replies: Iterable[RawCommentEvent],
             getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[RawCommentEvent] = {
      if (replies.isEmpty) acc // base case
      else loop(acc ++ replies)(replies.flatMap(getChildren), getChildren)
    }

    loop(Set())(replies, getChildren)
  }

  private def resolve(c: RawCommentEvent, postId: Long): CommentEvent =
    CommentEvent(
      c.commentId,
      c.personId,
      c.creationDate,
      c.locationIP,
      c.browserUsed,
      c.content,
      postId,
      c.replyToCommentId,
      c.placeId)
}

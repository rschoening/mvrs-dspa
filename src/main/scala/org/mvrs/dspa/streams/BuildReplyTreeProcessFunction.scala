package org.mvrs.dspa.streams

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Gauge}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.clustering.KMeansClusterFunction
import org.mvrs.dspa.model.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.streams.BuildReplyTreeProcessFunction._
import org.mvrs.dspa.utils.FlinkUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Process function to reconstruct the reply tree based on first-level comments (having a post reference) and
  * replies (not having a post reference).
  *
  * @param outputTagDroppedReplies optional output tag for dropped replies
  */
class BuildReplyTreeProcessFunction(outputTagDroppedReplies: Option[OutputTag[RawCommentEvent]])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]
    with CheckpointedFunction {

  // checkpointed operator state
  // - since state has to be accessed from the broadcast side also (outside of keyed context), unkeyed state is used.
  //   An alternative would be the use of applyToKeyedState(), however this processes all keyed states in sequence,
  //   which might become too slow if many keys are active
  @transient private lazy val danglingReplies = mutable.Map[Long, mutable.Set[RawCommentEvent]]()

  // TODO persist to ElasticSearch, use as cache (then no longer in operator state)
  @transient private lazy val postForComment: mutable.Map[Long, PostReference] = mutable.Map()

  @transient private var danglingRepliesListState: ListState[Map[Long, Set[RawCommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, PostReference]] = _

  // metrics
  @transient private var resolvedReplyCount: Counter = _
  @transient private var droppedReplyCount: Counter = _
  @transient private var danglingRepliesCount: Gauge[Int] = _
  @transient private var cacheProcessingTime: Gauge[Long] = _
  @transient private var cacheProcessingCount: Counter = _

  @transient private var danglingRepliesHistogram: DropwizardHistogramWrapper = _
  @transient private var cacheProcessingTimeHistogram: DropwizardHistogramWrapper = _

  @transient private var lastCacheProcessingTime = 0L

  @transient private var currentCommentWatermark: Long = _
  @transient private var currentBroadcastWatermark: Long = _
  @transient private var currentMinimumWatermark: Long = _

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[KMeansClusterFunction])

  override def open(parameters: Configuration): Unit = {
    // initialize transient variables
    currentCommentWatermark = Long.MinValue
    currentBroadcastWatermark = Long.MinValue
    currentMinimumWatermark = Long.MinValue

    // prepare metrics
    val group = getRuntimeContext.getMetricGroup

    resolvedReplyCount = group.counter("resolvedReplyCount")
    droppedReplyCount = group.counter("droppedReplyCount")
    cacheProcessingCount = group.counter("cacheProcessingCount")

    danglingRepliesCount = FlinkUtils.gaugeMetric("danglingRepliesCount", group, () => danglingReplies.size)
    cacheProcessingTime = FlinkUtils.gaugeMetric("cacheProcessingTime", group, () => lastCacheProcessingTime)

    danglingRepliesHistogram = FlinkUtils.histogramMetric("danglingRepliesHistogram", group)
    cacheProcessingTimeHistogram = FlinkUtils.histogramMetric("cacheProcessingTimeHistogram", group)
  }

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    assert(ctx.timerService().currentWatermark() == ctx.currentWatermark())
    assert(ctx.currentWatermark() >= currentCommentWatermark)
    if (firstLevelComment.timestamp <= ctx.currentWatermark()) {
      debug(s"Late element on comment stream: ${firstLevelComment.timestamp} <= ${ctx.currentWatermark()} " +
        s"($firstLevelComment)")
    }

    val postId = firstLevelComment.postId

    assert(ctx.getCurrentKey == postId) // must be keyed by post id

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    // NOTE how to deal with cache misses? Must be in this operator, however access to ElasticSearch
    // should be non-blocking
    postForComment(firstLevelComment.commentId) = PostReference(postId, firstLevelComment.timestamp)

    out.collect(firstLevelComment)

    // process all replies that were waiting for this comment (recursively)
    processWaitingChildren(
      firstLevelComment.commentId, firstLevelComment.timestamp, postId,
      out, reportDropped(_, ctx.output(_, _)))

    // advance the stored watermarks. If the minimum watermarks have advanced, then check for
    // dangling replies that can now be evicted
    currentCommentWatermark = ctx.currentWatermark()
    val minimumWatermark = math.min(currentCommentWatermark, currentBroadcastWatermark)
    if (minimumWatermark > currentMinimumWatermark) {
      // the minimum watermark has advanced, store it and process dangling replies
      currentMinimumWatermark = minimumWatermark

      evictDanglingReplies(currentMinimumWatermark, out, reportDropped(_, ctx.output(_, _)))
    }

    // register a timer (will call back at watermark for the timestamp)
    // NOTE actually the timer should be based on arriving unresolved replies, to evict them also if no first-level
    // comment arrives for a while. However the timer can only be registered in the keyed context of the
    // first-level comments.
    ctx.timerService().registerEventTimeTimer(firstLevelComment.timestamp)
  }

  override def processBroadcastElement(reply: RawCommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    // NOTE on broadcast stream, elements with timestamp == ctx.currentWatermark() are delivered
    // even if there are no actual late events

    assert(ctx.currentWatermark() >= currentBroadcastWatermark)
    assert(reply.replyToPostId.isEmpty)
    assert(reply.replyToCommentId.isDefined)

    // if a post reference for
    // look up post reference from map
    reply.replyToCommentId.flatMap(postForComment.get) match {
      case Some(postReference) =>
        // the parent comment was found in the map -> post id is known

        // If the parent comment has a later timestamp, the child reply (and all of its descendants) will be dropped,
        // to ensure deterministic results in spite of
        //  1) the non-deterministic arrival order of broadcast elements vs. primary elements, and
        //  2) the advancement of watermarks (which do so in a non-deterministic, proc-time dependent way)

        val replyBeforeParent = reply.timestamp < postReference.commentTimestamp

        val minimumValidTimestamp = if (replyBeforeParent) Long.MaxValue else reply.timestamp

        processWaitingChildren(reply.commentId, minimumValidTimestamp, postReference.postId,
          out, reportDropped(_, ctx.output(_, _)))

        if (replyBeforeParent) drop(reply, reportDropped(_, ctx.output(_, _)))
        else emit(createComment(reply, postReference.postId), out)

      case None => rememberDanglingReply(reply) // cache for later evaluation, globally in this operator/worker
    }

    // process dangling replies whenever watermark progressed
    currentBroadcastWatermark = if (ctx.currentWatermark() == Long.MinValue) ctx.currentWatermark() else ctx.currentWatermark() - 1 //
    val minimumWatermark = math.min(currentCommentWatermark, currentBroadcastWatermark)

    if (minimumWatermark > currentMinimumWatermark) {
      currentMinimumWatermark = minimumWatermark
      evictDanglingReplies(currentMinimumWatermark, out, reportDropped(_, ctx.output(_, _)))
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    // check if the minimum watermark has advanced. If so then check if any dangling replies can be evicted
    currentCommentWatermark = ctx.currentWatermark()

    val minimumWatermark = math.min(currentCommentWatermark, currentBroadcastWatermark)

    if (minimumWatermark > currentMinimumWatermark) {
      currentMinimumWatermark = minimumWatermark

      evictDanglingReplies(currentMinimumWatermark, out, reportDropped(_, ctx.output(_, _)))
    }
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

    val postForCommentDescriptor = new ListStateDescriptor[Map[Long, PostReference]](
      "post-for-comment",
      createTypeInformation[Map[Long, PostReference]])

    // NOTE UnionListState must be used so that state is guaranteed to be available after rescaling etc.
    // The implicitly sharded postForComment state will therefore be duplicated on all workers, requiring each
    // worker to be able to hold state about all past posts.
    // The problem is that the function does not know all the keys (post ids) it is responsible for. If it was then
    // It could discard the postForComment entries it does not need.
    // So the current solution should really be replaced with an central cache. This function would have to be taken
    // apart for this, into a preceding AsyncIO step doing the lookup for each incoming first-level comment and reply,
    // and a sink to upsert any changed mappings.
    danglingRepliesListState = context.getOperatorStateStore.getUnionListState(childRepliesDescriptor)
    postForCommentListState = context.getOperatorStateStore.getUnionListState(postForCommentDescriptor)

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

  private def emit(resolvedReply: CommentEvent, out: Collector[CommentEvent]): Unit = {
    // remember comment -> post mapping
    postForComment(resolvedReply.commentId) = PostReference(resolvedReply.postId, resolvedReply.timestamp)

    danglingReplies.remove(resolvedReply.commentId) // remove resolved replies from operator state

    out.collect(resolvedReply) // emit the resolved replies

    resolvedReplyCount.inc()
  }

  private def drop(reply: RawCommentEvent, report: RawCommentEvent => Unit): Unit = {
    // NOTE all replies waiting for this must have been processed FIRSTs
    postForComment(reply.commentId) = PostReference(Long.MinValue, Long.MaxValue)

    danglingReplies.remove(reply.commentId)
    report(reply)
    droppedReplyCount.inc()
  }

  private def processWaitingChildren(commentId: Long, earliestValidTimestamp: Long, postId: Long,
                                     collector: Collector[CommentEvent],
                                     reportDropped: RawCommentEvent => Unit) = {
    getDescendants(commentId, earliestValidTimestamp, getWaitingReplies) match {
      case Nil => // no replies

      case replies => replies.foreach {
        case (reply: RawCommentEvent, valid: Boolean) =>
          if (valid) emit(createComment(reply, postId), collector)
          else drop(reply, reportDropped)
      }
        danglingReplies.remove(commentId)
    }
  }

  private def reportDropped(reply: RawCommentEvent,
                            output: (OutputTag[RawCommentEvent], RawCommentEvent) => Unit): Unit =
    outputTagDroppedReplies.foreach(output(_, reply))

  private def evictDanglingReplies(minimumWatermark: Long,
                                   out: Collector[CommentEvent],
                                   reportDropped: RawCommentEvent => Unit): Unit = {
    val startTime = System.currentTimeMillis()

    for {(parentCommentId, replies) <- danglingReplies} {
      // The parent of a dangling reply can be either a first-level comment or another reply.
      // Only when the watermark of BOTH replies (broadcast) and first-level comments have passed
      // can we be sure that the reply won't ever find it's parent and that it can be dropped
      // -> use the minimum of the watermarks of both input streams

      val lostReplies = replies.filter(_.timestamp <= minimumWatermark)
      if (lostReplies.nonEmpty) {
        val lostRepliesWithChildren = getWithChildren(lostReplies, getWaitingReplies)

        lostRepliesWithChildren.foreach(t => drop(t, reportDropped))

        replies --= lostReplies // replies is *mutable* set

        if (replies.isEmpty) danglingReplies.remove(parentCommentId) // no remaining waiting replies
      }
    }

    // metrics
    lastCacheProcessingTime = System.currentTimeMillis() - startTime

    cacheProcessingTimeHistogram.update(lastCacheProcessingTime)

    cacheProcessingCount.inc()
  }

  private def getWaitingReplies(commentId: Long): List[RawCommentEvent] =
    danglingReplies.get(commentId).map(_.toList).getOrElse(List[RawCommentEvent]())

  private def getWaitingReplies(event: RawCommentEvent): mutable.Set[RawCommentEvent] =
    danglingReplies.getOrElse(event.commentId, mutable.Set[RawCommentEvent]())

  private def rememberDanglingReply(reply: RawCommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    danglingReplies.get(parentCommentId) match {
      case Some(replies) => replies += reply // mutable, updated in place
      case None => danglingReplies.put(parentCommentId, mutable.Set(reply))
    }

    danglingRepliesHistogram.update(danglingReplies.size)
  }

  private def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)
}

object BuildReplyTreeProcessFunction {

  def getWithChildren(replies: Iterable[RawCommentEvent],
                      getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[RawCommentEvent] = {
    @scala.annotation.tailrec
    def loop(acc: Set[RawCommentEvent])
            (replies: Iterable[RawCommentEvent],
             getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[RawCommentEvent] = {
      if (replies.isEmpty) acc // base case
      else loop(acc ++ replies)(replies.flatMap(getChildren), getChildren) // recurse, accumulate set
    }

    loop(Set())(replies, getChildren)
  }

  /**
    * Gets the descendants of a comment, with an indication if the descendant is valid with regard to causality
    * (timestamp along parent->child chain must increase monotonically)
    *
    * @param parentId        the id of the parent comment
    * @param parentTimestamp the timestamp of the parent comment
    * @param getChildren     funcdtion to get the immediate children based on a comment id
    * @return flattened list of child comments with a boolean indicating validity
    *         (valid children will be emitted, invalid children will be dropped)
    */
  def getDescendants(parentId: Long,
                     parentTimestamp: Long,
                     getChildren: Long => List[RawCommentEvent]): List[(RawCommentEvent, Boolean)] = {
    def childInfo(reply: RawCommentEvent, minTimestamp: Long, parentIsValid: Boolean): (RawCommentEvent, Long, Boolean) =
      (reply, math.max(minTimestamp, reply.timestamp), parentIsValid && reply.timestamp >= minTimestamp)

    @scala.annotation.tailrec
    def loop(acc: List[(RawCommentEvent, Long, Boolean)],
             replies: List[(RawCommentEvent, Long, Boolean)]): List[(RawCommentEvent, Long, Boolean)] =
      replies match {
        case Nil => acc // base case

        case (reply, minTimestamp, parentIsValid) :: siblings =>

          val newAcc = childInfo(reply, minTimestamp, parentIsValid) :: acc // new value for accumulator

          getChildren(reply.commentId) match {
            // the reply has children, go depth-first
            case children => loop(newAcc, children.map(childInfo(_, minTimestamp, parentIsValid)) ::: siblings)

            // no children, proceed with siblings
            case Nil => loop(newAcc, siblings)
          }
      }

    val replies = getChildren(parentId).map(childInfo(_, parentTimestamp, parentIsValid = true))

    loop(List(), replies).map(t => (t._1, t._3)) // start the recursion
  }

  private def createComment(c: RawCommentEvent, postId: Long): CommentEvent =
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

  case class PostReference(postId: Long, commentTimestamp: Long)

}

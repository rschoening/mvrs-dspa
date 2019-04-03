package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Meter}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.utils

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

class BuildReplyTreeProcessFunction(outputTagDroppedReplies: Option[OutputTag[CommentEvent]])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]
    with CheckpointedFunction {

  // checkpointed operator state
  private val danglingReplies: mutable.Map[Long, (Long, mutable.Set[CommentEvent])] = mutable.Map[Long, (Long, mutable.Set[CommentEvent])]()
  // TODO use a separate data structure for the parent keys and maximum timestamps, to quickly identify those that can be dropped (priority queue of (parent-id, timestamp)?)
  // TODO create class for danglingreplies, with merge operation when reading from list state, and with unit tests

  private val postForComment: mutable.Map[Long, Long] = mutable.Map()
  @transient private var danglingRepliesListState: ListState[Map[Long, Set[CommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, Long]] = _

  // metrics
  @transient private var firstLevelCommentCounter: Counter = _
  @transient private var replyCounter: Counter = _
  @transient private var resolvedReplyCount: Counter = _
  @transient private var droppedReplyCounter: Counter = _
  @transient private var throughputMeter: Meter = _

  private var maximumReplyTimestamp: Long = Long.MinValue
  private var currentCommentWatermark: Long = Long.MinValue
  private var currentBroadcastWatermark: Long = Long.MinValue
  private var currentMinimumWatermark: Long = Long.MinValue

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    firstLevelCommentCounter = group.counter("firstlevelcomment-counter")
    replyCounter = group.counter("reply-counter")
    resolvedReplyCount = group.counter("rootedreply-counter")
    droppedReplyCounter = group.counter("droppedreply-counter")
    throughputMeter = group.meter("throughput-meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
  }

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    firstLevelCommentCounter.inc()

    val postId = firstLevelComment.postId
    assert(ctx.getCurrentKey == postId) // must be keyed by post id

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    postForComment.put(firstLevelComment.id, postId)

    out.collect(firstLevelComment)
    throughputMeter.markEvent()

    // process all replies that were waiting for this comment (recursively)
    if (danglingReplies.contains(firstLevelComment.id)) {

      val resolved = BuildReplyTreeProcessFunction.resolveDanglingReplies(
        danglingReplies(firstLevelComment.id)._2, postId, getDanglingReplies)

      emitResolvedReplies(resolved, out)

      danglingReplies.remove(firstLevelComment.id)
    }

    if (ctx.currentWatermark() > currentCommentWatermark) {
      val newMinimum = math.min(ctx.currentWatermark(), currentCommentWatermark)
      val msg = s"Comment WM ${utils.formatTimestamp(currentCommentWatermark)} -> ${utils.formatTimestamp(ctx.currentWatermark())}"
      if (newMinimum > currentMinimumWatermark) {
        println(s"$msg >> minimum + ${utils.formatDuration(newMinimum - currentMinimumWatermark)} *********************")
        currentMinimumWatermark = newMinimum

        processDanglingReplies(currentMinimumWatermark, out, _ => ()) // TODO
      }
      else println(msg)
      currentCommentWatermark = ctx.currentWatermark()
    }

    // register a timer (will call back at watermark for the creationDate)
    // NOTE actually the timer should be based on arriving unresolved replies, however the timer can only be registered
    //      in the keyed context of the first-level comments.
    // NOTE without this timer, the unit tests all fail
    ctx.timerService().registerEventTimeTimer(firstLevelComment.creationDate)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    val reportDropped = outputTagDroppedReplies
      .map(t => (r: CommentEvent) => ctx.output(t, r))
      .getOrElse((_: CommentEvent) => ())

    processDanglingReplies(timestamp, out, reportDropped)
  }

  private def processDanglingReplies(timestamp: Long, out: Collector[CommentEvent], reportDroppedReply: CommentEvent => Unit): Unit = {
    // first, let all known parents resolve their children
    for {(parentCommentId, (_, replies)) <- danglingReplies} {
      // check if the post id for the parent comment id is now known
      val postId = postForComment.get(parentCommentId)

      if (postId.isDefined) {
        // the post id was found -> point all dangling children to it
        val resolved = BuildReplyTreeProcessFunction.resolveDanglingReplies(
          replies, postId.get, getDanglingReplies)

        emitResolvedReplies(resolved, out)

        danglingReplies.remove(parentCommentId)
      }
    }

    // process all remaining dangling replies
    for {(parentCommentId, (maxTimestamp, replies)) <- danglingReplies} {
      assert(postForComment.get(parentCommentId).isEmpty) // assert it's really dangling

      if (maxTimestamp <= timestamp) {
        // all dangling children are past the watermark -> parent is no longer expected -> drop replies
        val unresolved = BuildReplyTreeProcessFunction.getDanglingReplies(replies, getDanglingReplies)

        // NOTE with multiple workers, the unresolved replies are emitted for each worker! in the end it might be better to drop that side output, as it can be confusing. better gather metrics on size of state
        unresolved.foreach(r => {
          danglingReplies.remove(r.id) // remove resolved replies from operator state

          reportDroppedReply(r)

          droppedReplyCounter.inc()
        })

        // forget about the referenced comment also
        danglingReplies.remove(parentCommentId)
      }
    }
    println(s"${danglingReplies.size}")
  }

  private def emitResolvedReplies(resolved: Iterable[CommentEvent], out: Collector[CommentEvent]): Unit = {
    val count = resolved.size
    require(count > 0, "empty replies iterable")

    resolved.foreach(
      r => {
        postForComment(r.id) = r.postId // remember comment -> post mapping
        danglingReplies.remove(r.id) // remove resolved replies from operator state
        out.collect(r) // emit the resolved replies
      })

    // metrics
    throughputMeter.markEvent(count)
    resolvedReplyCount.inc(count)
  }

  private def getDanglingReplies(event: CommentEvent) =
    danglingReplies.getOrElse(event.id, (Long.MinValue, mutable.Set[CommentEvent]()))._2

  override def processBroadcastElement(reply: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    replyCounter.inc() // count per parallel worker

    maximumReplyTimestamp = math.max(maximumReplyTimestamp, reply.creationDate)

    val postId = postForComment.get(reply.replyToCommentId.get)

    if (postId.isDefined) {
      postForComment(reply.id) = postId.get // remember our new friend
      out.collect(reply.copy(replyToPostId = postId)) // ... and immediately emit the rooted reply
    }
    else rememberDanglingReply(reply) // cache for later evaluation, globally in this operator/worker

    if (ctx.currentWatermark() > currentBroadcastWatermark) {
      val newMinimum = math.min(ctx.currentWatermark(), currentBroadcastWatermark)
      val msg = s"Broadcast WM ${utils.formatTimestamp(currentBroadcastWatermark)} -> ${utils.formatTimestamp(ctx.currentWatermark())}"
      if (newMinimum > currentMinimumWatermark) {
        println(s"$msg >> minimum + ${utils.formatDuration(newMinimum - currentMinimumWatermark)}")
        currentMinimumWatermark = newMinimum
        processDanglingReplies(currentMinimumWatermark, out, _ => ())
      }
      else println(msg)
      currentBroadcastWatermark = ctx.currentWatermark()
    }

  }

  private def rememberDanglingReply(reply: CommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    val newValue =
      danglingReplies
        .get(parentCommentId)
        .map { case (ts, replies) => (math.max(ts, reply.creationDate), replies += reply) } // found, calc new value
        .getOrElse((reply.creationDate, mutable.Set(reply))) // initial value if not in map

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

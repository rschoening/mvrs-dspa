package org.mvrs.dspa.streams

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.{Counter, Gauge, Meter}
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
  private val danglingReplies: mutable.Map[Long, (Long, mutable.Set[RawCommentEvent])] = mutable.Map[Long, (Long, mutable.Set[RawCommentEvent])]()
  private val postForComment: mutable.Map[Long, Long] = mutable.Map() // TODO persist to ElasticSearch, use as cache (then no longer in operator state)

  @transient private var danglingRepliesListState: ListState[Map[Long, Set[RawCommentEvent]]] = _
  @transient private var postForCommentListState: ListState[Map[Long, Long]] = _

  // metrics
  // TODO remove redundant metrics (eg. throughput, row counts per input/output)
  @transient private var firstLevelCommentCounter: Counter = _
  @transient private var replyCounter: Counter = _
  @transient private var resolvedReplyCount: Counter = _
  @transient private var droppedReplyCounter: Counter = _
  @transient private var throughputMeter: Meter = _
  @transient private var danglingRepliesCount: Gauge[Int] = _

  // TODO revise watermark-related logic
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
    danglingRepliesCount = group.gauge[Int, ScalaGauge[Int]]("dangling-replies-gauge", ScalaGauge[Int](() => danglingReplies.size))
  }

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    firstLevelCommentCounter.inc()

    val postId = firstLevelComment.postId
    assert(ctx.getCurrentKey == postId) // must be keyed by post id

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    postForComment.put(firstLevelComment.commentId, postId)

    out.collect(firstLevelComment)
    throughputMeter.markEvent()

    // process all replies that were waiting for this comment (recursively)
    if (danglingReplies.contains(firstLevelComment.commentId)) {

      val resolved = resolveDanglingReplies(
        danglingReplies(firstLevelComment.commentId)._2, postId, getDanglingReplies)

      emitResolvedReplies(resolved, out)

      danglingReplies.remove(firstLevelComment.commentId)
    }

    // process dangling replies whenever watermark progressed
    currentCommentWatermark = processWatermark(
      ctx.currentWatermark(),
      currentCommentWatermark,
      out, (t, r) => ctx.output(t, r))

    // register a timer (will call back at watermark for the creationDate)
    // NOTE actually the timer should be based on arriving unresolved replies, however the timer can only be registered
    //      in the keyed context of the first-level comments.
    ctx.timerService().registerEventTimeTimer(firstLevelComment.timestamp)
  }

  private def processWatermark(newWatermark: Long, currentWatermark: Long,
                               collector: Collector[CommentEvent],
                               sideOutput: (OutputTag[RawCommentEvent], RawCommentEvent) => Unit): Long = {
    if (newWatermark > currentWatermark) {
      val newMinimum = math.min(newWatermark, currentWatermark)

      if (newMinimum > currentMinimumWatermark) {
        currentMinimumWatermark = newMinimum
        processDanglingReplies(currentMinimumWatermark, collector, reportDropped(_, sideOutput))
      }
    }

    newWatermark
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    processDanglingReplies(timestamp, out, reportDropped(_, (t, r) => ctx.output(t, r)))
  }

  override def processBroadcastElement(reply: RawCommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, RawCommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    replyCounter.inc() // count per parallel worker

    val postId = postForComment.get(reply.replyToCommentId.get)

    if (postId.isDefined) {
      postForComment(reply.commentId) = postId.get // remember our new friend

      out.collect(resolve(reply, postId.get)) // ... and immediately emit the rooted reply
      throughputMeter.markEvent()
    }
    else rememberDanglingReply(reply) // cache for later evaluation, globally in this operator/worker

    // process dangling replies whenever watermark progressed
    currentBroadcastWatermark = processWatermark(
      ctx.currentWatermark(), currentBroadcastWatermark,
      out, (t, r) => ctx.output(t, r))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    danglingRepliesListState.clear()
    danglingRepliesListState.add(danglingReplies.map(t => (t._1, t._2._2.toSet)).toMap)

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
          val earliestReplyTimestamp = replies.foldLeft(Long.MinValue)((z, r) => math.min(z, r.timestamp))
          val set = mutable.Set(replies.toSeq: _*)
          if (danglingReplies.get(parentCommentId).isEmpty) danglingReplies(parentCommentId) = (earliestReplyTimestamp, set)
          else danglingReplies(parentCommentId) = (earliestReplyTimestamp, set ++ danglingReplies(parentCommentId)._2)
        }
      }

      // merge the maps (comment -> post)
      postForComment.clear()
      postForCommentListState.get.asScala.foreach(postForComment ++= _)
    }
  }

  private def reportDropped(reply: RawCommentEvent, output: (OutputTag[RawCommentEvent], RawCommentEvent) => Unit): Unit =
    outputTagDroppedReplies.foreach(output(_, reply))

  private def processDanglingReplies(timestamp: Long, out: Collector[CommentEvent], reportDroppedReply: RawCommentEvent => Unit): Unit = {
    // first, let all known parents resolve their children
    for {(parentCommentId, (_, replies)) <- danglingReplies} {
      // check if the post id for the parent comment id is now known
      val postId = postForComment.get(parentCommentId)

      postId.foreach(id => {
        // the post id was found -> point all dangling children to it
        val resolved = BuildReplyTreeProcessFunction.resolveDanglingReplies(replies, id, getDanglingReplies)

        emitResolvedReplies(resolved, out)

        danglingReplies.remove(parentCommentId)
      })
    }

    // process all remaining dangling replies
    for {(parentCommentId, (earliestReplyTimestamp, replies)) <- danglingReplies} {
      if (earliestReplyTimestamp <= timestamp) {
        // the watermark has passed the earliest reply to this unknown parent. As the parent must have been before
        // that earliest reply, we can assume that the parent no longer arrives (as wm incorporates the defined bounded delay)
        // --> recursively drop all replies to this parent
        val unresolved = BuildReplyTreeProcessFunction.getDanglingReplies(replies, getDanglingReplies)

        // NOTE with multiple workers, the unresolved replies are emitted for each worker. Side output can
        //      still be useful for analysis with parallelism = 1
        unresolved.foreach(r => {
          danglingReplies.remove(r.commentId) // remove resolved replies from operator state

          reportDroppedReply(r)

          droppedReplyCounter.inc()
        })

        // forget about the referenced comment also
        danglingReplies.remove(parentCommentId)
      }
    }
  }

  private def emitResolvedReplies(resolved: Iterable[CommentEvent], out: Collector[CommentEvent]): Unit = {
    val count = resolved.size
    require(count > 0, "empty replies iterable")

    resolved.foreach(
      r => {
        postForComment(r.commentId) = r.postId // remember comment -> post mapping
        danglingReplies.remove(r.commentId) // remove resolved replies from operator state
        out.collect(r) // emit the resolved replies
      })

    // metrics
    throughputMeter.markEvent(count)
    resolvedReplyCount.inc(count)
  }

  private def getDanglingReplies(event: RawCommentEvent) =
    danglingReplies.getOrElse(event.commentId, (Long.MinValue, mutable.Set[RawCommentEvent]()))._2

  private def rememberDanglingReply(reply: RawCommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    val newValue =
      danglingReplies
        .get(parentCommentId)
        .map { case (ts, replies) => (math.min(ts, reply.timestamp), replies += reply) } // found, calc new value
        .getOrElse((reply.timestamp, mutable.Set(reply))) // initial value if not in map

    danglingReplies.put(parentCommentId, newValue)
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
    def loop(acc: Set[RawCommentEvent])(replies: Iterable[RawCommentEvent],
                                        getChildren: RawCommentEvent => Iterable[RawCommentEvent]): Set[RawCommentEvent] = {
      if (replies.isEmpty) acc // base case
      else loop(acc ++ replies)(replies.flatMap(getChildren), getChildren)
    }

    loop(Set())(replies, getChildren)
  }

  private def resolve(c: RawCommentEvent, postId: Long): CommentEvent = {
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

}

package org.mvrs.dspa.preparation

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
import org.mvrs.dspa.events.CommentEvent

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
  @transient private var processingDanglingRepliesCounter: Counter = _
  @transient private var throughputMeter: Meter = _
  @transient private var danglingRepliesCount: Gauge[Int] = _
  @transient private var processDanglingRepliesNanos0: Gauge[Double] = _
  @transient private var processDanglingRepliesNanos1: Gauge[Double] = _

  // private var maximumReplyTimestamp: Long = Long.MinValue
  // private var currentCommentWatermark: Long = Long.MinValue
  private var currentBroadcastWatermark: Long = Long.MinValue
  private var currentMinimumWatermark: Long = Long.MinValue
  private var lastProcessDanglingRepliesNanos0: Double = 0L
  private var lastProcessDanglingRepliesNanos1: Double = 0L

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    firstLevelCommentCounter = group.counter("firstlevelcomment-counter")
    replyCounter = group.counter("reply-counter")
    resolvedReplyCount = group.counter("rootedreply-counter")
    droppedReplyCounter = group.counter("droppedreply-counter")
    processingDanglingRepliesCounter = group.counter("process-dangling-replies-counter")
    throughputMeter = group.meter("throughput-meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
    danglingRepliesCount = group.gauge[Int, ScalaGauge[Int]]("dangling-replies-gauge", ScalaGauge[Int](() => danglingReplies.size))
    processDanglingRepliesNanos0 = group.gauge[Double, ScalaGauge[Double]]("process-dangling-replies-0-gauge", ScalaGauge[Double](() => lastProcessDanglingRepliesNanos0))
    processDanglingRepliesNanos1 = group.gauge[Double, ScalaGauge[Double]]("process-dangling-replies-1-gauge", ScalaGauge[Double](() => lastProcessDanglingRepliesNanos1))
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

    // process dangling replies whenever watermark progressed
//    currentCommentWatermark = processWatermark(
//      ctx.currentWatermark(),
//      currentCommentWatermark,
//      out, (t, r) => ctx.output(t, r))

    // register a timer (will call back at watermark for the creationDate)
    // NOTE actually the timer should be based on arriving unresolved replies, however the timer can only be registered
    //      in the keyed context of the first-level comments.
    ctx.timerService().registerEventTimeTimer(firstLevelComment.creationDate)
  }

  private def processWatermark(newWatermark: Long, currentWatermark: Long,
                               collector: Collector[CommentEvent],
                               sideOutput: (OutputTag[CommentEvent], CommentEvent) => Unit): Long = {
    if (newWatermark > currentWatermark) {
      val newMinimum = math.min(newWatermark, currentWatermark)

      // val msg = s"Comment WM ${utils.formatTimestamp(currentCommentWatermark)} -> ${utils.formatTimestamp(ctx.currentWatermark())}"

      if (newMinimum > currentMinimumWatermark) {
        // println(s"$msg >> minimum + ${utils.formatDuration(newMinimum - currentMinimumWatermark)} *********************")
        currentMinimumWatermark = newMinimum
        processDanglingReplies(currentMinimumWatermark, collector, reportDropped(_, sideOutput))
      }
      // else println(msg)
    }

    newWatermark
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#OnTimerContext,
                       out: Collector[CommentEvent]): Unit = {
    processDanglingReplies(timestamp, out, reportDropped(_, (t, r) => ctx.output(t, r)))
  }

  override def processBroadcastElement(reply: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    replyCounter.inc() // count per parallel worker

    // maximumReplyTimestamp = math.max(maximumReplyTimestamp, reply.creationDate)

    val postId = postForComment.get(reply.replyToCommentId.get)

    if (postId.isDefined) {
      postForComment(reply.id) = postId.get // remember our new friend
      out.collect(reply.copy(replyToPostId = postId)) // ... and immediately emit the rooted reply
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
          val earliestReplyTimestamp = replies.foldLeft(Long.MinValue)((z, r) => math.min(z, r.creationDate))
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

  private def reportDropped(reply: CommentEvent, output: (OutputTag[CommentEvent], CommentEvent) => Unit): Unit =
    outputTagDroppedReplies.foreach(output(_, reply))

  private def processDanglingReplies(timestamp: Long, out: Collector[CommentEvent], reportDroppedReply: CommentEvent => Unit): Unit = {
    processingDanglingRepliesCounter.inc()

    val startTime = System.nanoTime()

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

    // measure elapsed time, to report in gauge
    lastProcessDanglingRepliesNanos0 = (System.nanoTime() - startTime) / 1000000.0

    val t1 = System.nanoTime()

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
          danglingReplies.remove(r.id) // remove resolved replies from operator state

          reportDroppedReply(r)

          droppedReplyCounter.inc()
        })

        // forget about the referenced comment also
        danglingReplies.remove(parentCommentId)
      }
    }

    lastProcessDanglingRepliesNanos1 = (System.nanoTime() - t1) / 1000000.0
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

  private def rememberDanglingReply(reply: CommentEvent): Unit = {
    val parentCommentId = reply.replyToCommentId.get // must not be None

    val newValue =
      danglingReplies
        .get(parentCommentId)
        .map { case (ts, replies) => (math.min(ts, reply.creationDate), replies += reply) } // found, calc new value
        .getOrElse((reply.creationDate, mutable.Set(reply))) // initial value if not in map

    danglingReplies.put(parentCommentId, newValue)
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

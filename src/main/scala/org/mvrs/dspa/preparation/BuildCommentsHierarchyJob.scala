package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.functions.ScaledReplayFunction
import org.mvrs.dspa.preparation.LoadCommentEventsJob.ParseError
import org.mvrs.dspa.utils

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(10L) // NOTE this is REQUIRED for timers to fire, apparently

  val outputTagParsingErrors = new OutputTag[ParseError]("comment parsing errors")

  val allComments = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line? use table api csv source and convert to datastream?
    .map(CommentEvent.parse _) // TODO use parser process function with side output for errors
    .process(new ScaledReplayFunction[CommentEvent](_.creationDate, 10000, 0))
    .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](Time.milliseconds(1), _.creationDate))

  val postComments = allComments
    .filter(_.replyToPostId.isDefined)
    .keyBy(_.postId)

  val repliesBroadcast = allComments
    .filter(_.replyToPostId.isEmpty)
    .broadcast()

  val outputTagDroppedReplies = new OutputTag[CommentEvent]("dropped replies")

  val hierarchyBuilder = postComments.connect(repliesBroadcast)
  val rootedComments = hierarchyBuilder.process(new BuildReplyTreeProcessFunction(outputTagDroppedReplies))

  // val droppedReplies = rootedComments.getSideOutput(outputTagDroppedReplies)

  val plan = env.getExecutionPlan

  //  var allCount = 0
  //  allComments.map(_ => allCount += 1)
  //
  //  var droppedCount = 0
  //  droppedReplies.map(_ => droppedCount += 1)

  //  var rootedCount = 0
  //  rootedComments.map(_ => rootedCount += 1)

  // rootedComments.print

  env.execute()

  //  println(s"All comments: $allCount")
  //  println(s"Rooted comments: $rootedCount")
  //  println(s"Dropped replies: $droppedCount")
  println(plan)
}

class BuildReplyTreeProcessFunction(outputTagDroppedEvents: OutputTag[CommentEvent])
  extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]
    with CheckpointedFunction {

  private lazy val postForComment: MapState[Long, Long] = getRuntimeContext.getMapState(postForCommentState)
  // actually the set of comment ids that refer to a post is sufficient, could do with ValueState[Set[Long]] - however MapState may be more efficient for checkpoints etc.?
  private val danglingReplies: mutable.Map[Long, mutable.Set[CommentEvent]] = mutable.Map() // checkpointed operator state
  private val postForCommentState = new MapStateDescriptor[Long, Long](
    "postForComment",
    createTypeInformation[Long],
    createTypeInformation[Long])

  @transient
  private var danglingRepliesListState: ListState[Map[Long, Set[CommentEvent]]] = _

  override def processElement(firstLevelComment: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    val postId = firstLevelComment.postId
    assert(ctx.getCurrentKey == postId)

    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    // TODO consider using unkeyed operator state? simplifies operator-scoped lookup
    // TODO on the other hand, using keyed state would allow keeping simply a Set of comment ids instead of the mapping
    postForComment.put(firstLevelComment.id, postId)

    // process all replies that were waiting for this comment (recursively)
    if (danglingReplies.contains(firstLevelComment.id)) {

      val resolved = resolveDanglingReplies(Set())(
        danglingReplies(firstLevelComment.id),
        postId,
        reply => danglingReplies.getOrElse(reply.id, mutable.Set()))

      resolved.foreach(c => danglingReplies.remove(c.id)) // remove resolved replies from operator state

      // emit the resolved replies
      resolved.foreach(out.collect)
    }
  }

  override def processBroadcastElement(reply: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    val parentCommentId = reply.replyToCommentId.get

    // NOTE this is VERY slow - switch to operator state (list state resolving to unioned comment_id -> post_id)

    val postId = lookupPostId(parentCommentId, ctx) // this checks all keyed states in this operator

    if (postId.isDefined) out.collect(reply.copy(replyToPostId = postId))
    else {
      // cache for later evaluation, globally in this operator
      if (danglingReplies.contains(parentCommentId)) danglingReplies(parentCommentId) += reply
      else danglingReplies.put(parentCommentId, mutable.Set(reply))
    }

    // evict all dangling replies older than the watermark
    val watermark = ctx.currentWatermark()

    println(s"${getRuntimeContext.getIndexOfThisSubtask}: watermark: ${utils.formatTimestamp(watermark)} - reply: ${utils.formatTimestamp(reply.creationDate)} - diff: ${(reply.creationDate - watermark) / (1000f * 60f)} min")

    // this is also slow, since danglingReplies grows (to be investigated)
    for {replies <- danglingReplies} {
      val lostReplies = replies._2.filter(_.creationDate <= watermark) // parent will probably no longer arrive

      if (lostReplies.nonEmpty) {
        replies._2 --= lostReplies // remove from mutable set
        lostReplies.foreach(ctx.output(outputTagDroppedEvents, _)) // emit dropped replies to side output
      }
    }

    danglingReplies.retain((_, v) => v.nonEmpty)
    println(s"  remaining operator state size: ${danglingReplies.size}")
  }

  private def lookupPostId(parentCommentId: Long,
                           ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context): Option[Long] = {
    var postId: Option[Long] = None
    ctx.applyToKeyedState(postForCommentState, (key: Long, state: MapState[Long, Long]) =>
      if (postId.isEmpty && state.contains(parentCommentId)) postId = Some(key))
    postId
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    danglingRepliesListState.clear()
    danglingRepliesListState.add(danglingReplies.map(t => (t._1, t._2.toSet)).toMap)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Map[Long, Set[CommentEvent]]](
      "child-replies",
      createTypeInformation[Map[Long, Set[CommentEvent]]]
    )

    danglingRepliesListState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      danglingReplies.clear()

      // merge maps
      for (element <- danglingRepliesListState.get().asScala) {
        for {(parentCommentId, children) <- element} {
          val set = mutable.Set(children.toSeq: _*)
          if (danglingReplies.get(parentCommentId).isEmpty) danglingReplies(parentCommentId) = set
          else danglingReplies(parentCommentId) = set ++ danglingReplies(parentCommentId)
        }
      }
    }
  }

  @tailrec
  private def resolveDanglingReplies(acc: Set[CommentEvent])(replies: Iterable[CommentEvent],
                                                             postId: Long,
                                                             getChildren: CommentEvent => mutable.Set[CommentEvent]): Set[CommentEvent] = {
    if (replies.isEmpty) acc
    else {
      val resolvedReplies = replies.map(_.copy(replyToPostId = Some(postId)))

      resolveDanglingReplies(acc ++ resolvedReplies)(resolvedReplies.flatMap(getChildren(_)), postId, getChildren)
    }
  }
}


//class BuildHierarchyProcessFunction extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent] {
//  private lazy val postForComment: MapState[Long, Long] = getRuntimeContext.getMapState(postForCommentState)
//  private lazy val childReplies: MapState[Long, mutable.MutableList[CommentEvent]] = getRuntimeContext.getMapState(childRepliesState)
//  private val postForCommentState = new MapStateDescriptor[Long, Long](
//    "postForComment",
//    createTypeInformation[Long],
//    createTypeInformation[Long])
//
//
//  // TODO this should be UNKEYED
//  private val childRepliesState = new MapStateDescriptor[Long, mutable.MutableList[CommentEvent]](
//    "childReplies",
//    createTypeInformation[Long],
//    createTypeInformation[mutable.MutableList[CommentEvent]])
//
//  // TODO TTL definition for childRepliesState
//
//  override def processElement(value: CommentEvent,
//                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
//                              out: Collector[CommentEvent]): Unit = {
//    // this state is unbounded, replies may refer to arbitrarily old comments
//    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
//    postForComment.put(value.id, value.postId)
//
//    // reevaluate cached comments recursively (until no orphans found)
//    if (childReplies.contains(value.id)) {
//      val children = childReplies.get(value.id)
//      childReplies.remove(value.id)
//
//      val resolved = collectChildren(List())(
//        children,
//        c => if (childReplies.contains(c.id)) childReplies.get(c.id).toList else Nil)
//
//      resolved.foreach(c => postForComment.put(c.id, c.postId))
//      resolved.foreach(c => childReplies.remove(c.id))
//      resolved.foreach(out.collect)
//    }
//
//    // drop comments for which the watermark has passed - parent will not arrive anymore
//    //       if (ctx.timerService().currentWatermark() > xyz) { }
//
//  }
//
//  override def processBroadcastElement(value: CommentEvent,
//                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
//                                       out: Collector[CommentEvent]): Unit = {
//    val parentCommentId = value.replyToCommentId.get
//
//    // TODO need to set a timer to do this evaluation (in processElement)
//
//    ctx.applyToKeyedState(postForCommentState, (key: Long, state: MapState[Long, Long]) => ())
//
//    // TODO does not work yet, as the keyed state is not accessible from broadcast
//    if (postForComment.contains(parentCommentId)) {
//      out.collect(value.copy(replyToPostId = Some(postForComment.get(parentCommentId))))
//    }
//    else {
//      // cache for later evaluation
//      if (childReplies.contains(parentCommentId)) {
//        val children = childReplies.get(parentCommentId)
//        children += value
//        childReplies.put(parentCommentId, children)
//      }
//      else {
//        childReplies.put(parentCommentId, mutable.MutableList(value))
//      }
//    }
//  }
//
//  @tailrec
//  private def collectChildren(acc: List[CommentEvent])(parents: Seq[CommentEvent], getChildren: CommentEvent => List[CommentEvent]): List[CommentEvent] =
//    parents match {
//      case Nil => acc
//      case xs => val resolved = xs.flatMap(p => getChildren(p)
//        .map(_.copy(replyToPostId = Some(p.postId))))
//
//        collectChildren(acc ++ resolved)(resolved, getChildren)
//    }
//
//
//  //  @tailrec
//  //  private def collectChildren(resolvedChildren: List[CommentEvent])(parent: CommentEvent): List[CommentEvent] = {
//  //    if (!childReplies.contains(parent.id)) {
//  //      resolvedChildren
//  //    }
//  //    else {
//  //      val children = childReplies.get(parent.id)
//  //      childReplies.remove(parent.id) // at end?
//  //
//  //      for {child <- children} {
//  //        val rootedChild = child.copy(replyToPostId = Some(parent.postId))
//  //
//  //        postForComment.put(rootedChild.id, parent.postId)
//  //      }
//  //      collectChildren(List())(rootedChild)
//  //    }
//  //  }
//}

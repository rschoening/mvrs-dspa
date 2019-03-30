package org.mvrs.dspa.preparation

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.mvrs.dspa.events.CommentEvent
import org.mvrs.dspa.preparation.LoadCommentEventsJob.ParseError

import scala.annotation.tailrec
import scala.collection.mutable

object BuildCommentsHierarchyJob extends App {
  require(args.length == 1, "full path to csv file expected") // use scallop if more parameters needed

  val filePath: String = args(0)
  val kafkaTopic = "comments"
  val kafkaBrokers = "localhost:9092"

  // set up the streaming execution environment
  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(5)

  val outputTag = new OutputTag[ParseError]("comment parsing errors")

  val allComments = env
    .readTextFile(filePath)
    .filter(!_.startsWith("id|")) // TODO better way to skip the header line?
    .map(CommentEvent.parse _)

  val postComments = allComments
    .filter(_.replyToPostId.isDefined)
    .keyBy(_.postId)
  val repliesBroadcast = allComments.filter(_.replyToPostId.isEmpty).broadcast()

  val hierarchyBuilder = postComments.connect(repliesBroadcast)

  val rootedComments = hierarchyBuilder.process(new BuildHierarchyProcessFunction)

  rootedComments.print

  val plan = env.getExecutionPlan

  env.execute()

  println(plan)
}

class BuildHierarchyProcessFunction extends KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent] {
  private lazy val postForComment: MapState[Long, Long] = getRuntimeContext.getMapState(postForCommentState)
  private lazy val childReplies: MapState[Long, mutable.MutableList[CommentEvent]] = getRuntimeContext.getMapState(childRepliesState)
  private val postForCommentState = new MapStateDescriptor[Long, Long](
    "postForComment",
    createTypeInformation[Long],
    createTypeInformation[Long])


  // TODO this should be UNKEYED
  private val childRepliesState = new MapStateDescriptor[Long, mutable.MutableList[CommentEvent]](
    "childReplies",
    createTypeInformation[Long],
    createTypeInformation[mutable.MutableList[CommentEvent]])

  // TODO TTL definition for childRepliesState

  override def processElement(value: CommentEvent,
                              ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#ReadOnlyContext,
                              out: Collector[CommentEvent]): Unit = {
    // this state is unbounded, replies may refer to arbitrarily old comments
    // consider storing it in ElasticSearch, with a LRU cache maintained in the operator
    postForComment.put(value.id, value.postId)

    // reevaluate cached comments recursively (until no orphans found)
    if (childReplies.contains(value.id)) {
      val children = childReplies.get(value.id)
      childReplies.remove(value.id)

      val resolved = collectChildren(List())(
        children,
        c => if (childReplies.contains(c.id)) childReplies.get(c.id).toList else Nil)

      resolved.foreach(c => postForComment.put(c.id, c.postId))
      resolved.foreach(c => childReplies.remove(c.id))
      resolved.foreach(out.collect)
    }

    // drop comments for which the watermark has passed - parent will not arrive anymore
    //       if (ctx.timerService().currentWatermark() > xyz) { }

  }

  override def processBroadcastElement(value: CommentEvent,
                                       ctx: KeyedBroadcastProcessFunction[Long, CommentEvent, CommentEvent, CommentEvent]#Context,
                                       out: Collector[CommentEvent]): Unit = {
    val parentCommentId = value.replyToCommentId.get

    // TODO need to set a timer to do this evaluation (in processElement)

    ???
    // ctx.applyToKeyedState()

    // TODO does not work yet, as the keyed state is not accessible from broadcast
    if (postForComment.contains(parentCommentId)) {
      out.collect(value.copy(replyToPostId = Some(postForComment.get(parentCommentId))))
    }
    else {
      // cache for later evaluation
      if (childReplies.contains(parentCommentId)) {
        val children = childReplies.get(parentCommentId)
        children += value
        childReplies.put(parentCommentId, children)
      }
      else {
        childReplies.put(parentCommentId, mutable.MutableList(value))
      }
    }
  }

  @tailrec
  private def collectChildren(acc: List[CommentEvent])(parents: Seq[CommentEvent], getChildren: CommentEvent => List[CommentEvent]): List[CommentEvent] =
    parents match {
      case Nil => acc
      case xs => val resolved = xs.flatMap(p => getChildren(p)
        .map(_.copy(replyToPostId = Some(p.postId))))

        collectChildren(acc ++ resolved)(resolved, getChildren)
    }


  //  @tailrec
  //  private def collectChildren(resolvedChildren: List[CommentEvent])(parent: CommentEvent): List[CommentEvent] = {
  //    if (!childReplies.contains(parent.id)) {
  //      resolvedChildren
  //    }
  //    else {
  //      val children = childReplies.get(parent.id)
  //      childReplies.remove(parent.id) // at end?
  //
  //      for {child <- children} {
  //        val rootedChild = child.copy(replyToPostId = Some(parent.postId))
  //
  //        postForComment.put(rootedChild.id, parent.postId)
  //      }
  //      collectChildren(List())(rootedChild)
  //    }
  //  }
}

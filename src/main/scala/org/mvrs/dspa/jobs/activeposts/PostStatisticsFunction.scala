package org.mvrs.dspa.jobs.activeposts

import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.mvrs.dspa.jobs.activeposts.PostStatisticsFunction._
import org.mvrs.dspa.model.{Event, EventType, PostStatistics}
import org.mvrs.dspa.utils.DateTimeUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Keyed process function to calculate post statistics within a sliding window
  *
  * @param windowSize      the window size
  * @param slide           the window slide
  * @param stateTtl        the time-to-live for buckets (in processing time, i.e. taking into account any
  * @param countPostAuthor indicates if the original post author should be counted as one of the involved users
  */
class PostStatisticsFunction(windowSize: Time, slide: Time, stateTtl: Time, countPostAuthor: Boolean = true)
  extends KeyedProcessFunction[Long, Event, PostStatistics] {

  // state
  @transient private lazy val lastActivityState = getRuntimeContext.getState(lastActivityDescriptor)
  @transient private lazy val windowEndState = getRuntimeContext.getState(windowEndDescriptor)
  @transient private lazy val bucketMapState = getRuntimeContext.getMapState(bucketMapDescriptor)

  // state descriptors
  @transient private lazy val bucketMapDescriptor = new MapStateDescriptor[Long, Bucket]("buckets", createTypeInformation[Long], createTypeInformation[Bucket])
  @transient private lazy val lastActivityDescriptor = new ValueStateDescriptor("lastActivity", classOf[Long])
  @transient private lazy val windowEndDescriptor = new ValueStateDescriptor("windowEnd", classOf[Long])

  @transient private lazy val LOG = LoggerFactory.getLogger(classOf[PostStatisticsFunction])

  override def open(parameters: Configuration): Unit = {
    debug(s"Time-to-live: ${DateTimeUtils.formatDuration(stateTtl.toMilliseconds)}")

    val ttlConfig = StateTtlConfig
      .newBuilder(stateTtl)
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    bucketMapDescriptor.enableTimeToLive(ttlConfig)
    lastActivityDescriptor.enableTimeToLive(ttlConfig)
    windowEndDescriptor.enableTimeToLive(ttlConfig)
  }

  override def onTimer(windowExclusiveUpperBound: Long,
                       ctx: KeyedProcessFunction[Long, Event, PostStatistics]#OnTimerContext,
                       out: Collector[PostStatistics]): Unit = {

    // NOTE: window start is inclusive, end is exclusive (=> start of next window)
    // NOTE: bucket timestamp is exclusive upper bound of bucket window (window size equals slide of full window)

    val lastActivity = lastActivityState.value()

    debug(s"onTimer: $windowExclusiveUpperBound for key: ${ctx.getCurrentKey} " +
      s"(last activity: $lastActivity window size: ${windowSize.toMilliseconds}")

    val windowInclusiveStartTime = windowExclusiveUpperBound - windowSize.toMilliseconds

    if (lastActivity < windowInclusiveStartTime) {
      // Last activity is before start of window. Nothing to report, now new timer to register
      debug("- last activity outside of window - go to sleep until next event arrives")

      bucketMapState.clear()
      windowEndState.clear()
      lastActivityState.clear()
    }
    else {
      debug("- last activity inside of window")

      // event counts
      val bucketsToDrop = mutable.MutableList[Long]()
      var commentCount = 0
      var replyCount = 0
      var likeCount = 0
      var postCreatedInWindow = false

      val activeUserSet = mutable.Set[Long]()

      bucketMapState.iterator().asScala.foreach(
        entry => {
          val bucketExclusiveUpperBound = entry.getKey

          if (bucketExclusiveUpperBound > windowExclusiveUpperBound) {
            // future bucket, ignore
          }
          else if (bucketExclusiveUpperBound <= windowInclusiveStartTime) {
            // bucket end timestamp is before window start time, to be evicted
            bucketsToDrop += bucketExclusiveUpperBound
          }
          else {
            // bucket within window
            val bucket = entry.getValue

            commentCount += bucket.commentCount
            replyCount += bucket.replyCount
            likeCount += bucket.likeCount

            if (bucket.originalPost) postCreatedInWindow = true

            // add bucket persons to unioned set for entire window
            activeUserSet ++= bucket.persons
          }
        })

      if (postCreatedInWindow || (commentCount + replyCount + likeCount > 0)) {
        if (countPostAuthor) assert(activeUserSet.nonEmpty)

        out.collect(
          PostStatistics(
            ctx.getCurrentKey, windowExclusiveUpperBound,
            commentCount, replyCount, likeCount,
            activeUserSet.size, postCreatedInWindow))
      }

      debug(s"registering FOLLOWING timer for $windowExclusiveUpperBound + $slide " +
        s"(current watermark: ${ctx.timerService.currentWatermark()})")

      registerWindowEndTimer(ctx.timerService, windowExclusiveUpperBound + slide.toMilliseconds)

      // evict state
      bucketsToDrop.foreach(bucketMapState.remove)
    }
  }

  private def registerWindowEndTimer(timerService: TimerService,
                                     endTime: Long): Unit = {
    windowEndState.update(endTime)
    timerService.registerEventTimeTimer(endTime)
  }

  override def processElement(value: Event,
                              ctx: KeyedProcessFunction[Long, Event, PostStatistics]#Context,
                              out: Collector[PostStatistics]): Unit = {
    if (windowEndState.value == 0) {
      // no window yet: register timer at event timestamp + slide
      debug(s"registering NEW timer for ${value.timestamp} + ${slide.toMilliseconds} " +
        s"(current watermark: ${ctx.timerService.currentWatermark()})")

      registerWindowEndTimer(ctx.timerService, value.timestamp + slide.toMilliseconds)
    }

    // register last activity for post
    lastActivityState.update(value.timestamp)

    val windowEnd = windowEndState.value
    val bucketTimestamp = getBucketForTimestamp(value.timestamp, windowEnd, slide.toMilliseconds)

    value.eventType match {
      case EventType.Comment => updateBucket(bucketTimestamp, _.addComment(value.personId))
      case EventType.Reply => updateBucket(bucketTimestamp, _.addReply(value.personId))
      case EventType.Like => updateBucket(bucketTimestamp, _.addLike(value.personId))
      case EventType.Post => updateBucket(bucketTimestamp, _.registerPost(value.personId))
    }

    if (value.timestamp > windowEnd) {
      debug(s"Event to future bucket: $value (current window: $windowEnd)")
    }
    else if (value.timestamp < windowEnd - windowSize.toMilliseconds) {
      debug(s"Event to past bucket: $value (current window: $windowEnd)")
    }
    else debug(s"Event to current bucket: $value")
  }

  private def debug(msg: => String): Unit = if (LOG.isDebugEnabled) LOG.debug(msg)

  private def updateBucket(bucketTimestamp: Long, action: Bucket => Unit): Unit = {
    val bucket = getBucket(bucketTimestamp)
    action(bucket)
    bucketMapState.put(bucketTimestamp, bucket)
  }

  private def getBucket(bucketTimestamp: Long) = {
    if (bucketMapState.contains(bucketTimestamp))
      bucketMapState.get(bucketTimestamp)
    else {
      val newBucket = Bucket(countPostAuthor)
      bucketMapState.put(bucketTimestamp, newBucket)
      newBucket
    }
  }
}

object PostStatisticsFunction {
  def getBucketForTimestamp(timestamp: Long, exclusiveUpperBound: Long, bucketSize: Long): Long = {
    require(bucketSize > 0, "invalid bucket size")

    val offset = exclusiveUpperBound % bucketSize
    val index = math.floor((timestamp - offset).toDouble / bucketSize).toLong

    offset + (index + 1) * bucketSize
  }

  // NOTE: to ensure serializability, case classes should be in companion object.
  // This ensures that there are no references to containing class
  case class Bucket(countPostAuthor: Boolean) {
    val persons: mutable.Set[Long] = mutable.Set()
    var commentCount: Int = 0
    var replyCount: Int = 0
    var likeCount: Int = 0
    var originalPost: Boolean = false

    def registerPost(personId: Long): Unit = {
      originalPost = true
      if (countPostAuthor) persons += personId
    }

    def addReply(personId: Long): Unit = {
      replyCount += 1
      persons += personId
    }

    def addLike(personId: Long): Unit = {
      likeCount += 1
      persons += personId
    }

    def addComment(personId: Long): Unit = {
      commentCount += 1
      persons += personId
    }
  }

}

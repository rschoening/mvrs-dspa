package org.mvrs.dspa.activeposts

import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object PostStatisticsFunction {
  def getBucketForTimestamp(timestamp: Long, exclusiveUpperBound: Long, bucketSize: Long): Long = {
    require(bucketSize > 0)

    val offset = exclusiveUpperBound % bucketSize
    val index = 1 + (timestamp - offset) / bucketSize
    offset + index * bucketSize
  }
}

class PostStatisticsFunction(windowSize: Long, slide: Long)
  extends KeyedProcessFunction[Long, Event, PostStatistics] {
  require(slide > 0, "slide must be > 0")
  require(windowSize > 0, "windowSize must be > 0")

  private lazy val lastActivityState = getRuntimeContext.getState(lastActivityDescriptor)
  private lazy val windowEndState = getRuntimeContext.getState(windowEndDescriptor)
  private lazy val bucketMapState = getRuntimeContext.getMapState(bucketMapDescriptor)

  private val bucketMapDescriptor = new MapStateDescriptor[Long, Bucket]("buckets", classOf[Long], classOf[Bucket])
  private val lastActivityDescriptor = new ValueStateDescriptor("lastActivity", classOf[Long])
  private val windowEndDescriptor = new ValueStateDescriptor("windowEnd", classOf[Long])

  private val scaledTtlTime = org.apache.flink.api.common.time.Time.milliseconds(windowSize) // TODO
  private val ttlConfig = StateTtlConfig
    .newBuilder(scaledTtlTime)
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build

  private val LOG = LoggerFactory.getLogger(classOf[PostStatisticsFunction])

  override def open(parameters: Configuration): Unit = {
    bucketMapDescriptor.enableTimeToLive(ttlConfig)
    lastActivityDescriptor.enableTimeToLive(ttlConfig)
    windowEndDescriptor.enableTimeToLive(ttlConfig)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, Event, PostStatistics]#OnTimerContext,
                       out: Collector[PostStatistics]): Unit = {
    // NOTE: window start is inclusive, end is exclusive (=> start of next window)

    val lastActivity = lastActivityState.value()

    LOG.debug(s"onTimer: $timestamp for key: ${ctx.getCurrentKey} (last activity: $lastActivity window size: $windowSize")

    if (lastActivity < timestamp - windowSize) {
      // nothing to report, now new timer to register
      LOG.debug(s"- last activity outside of window - go to sleep until next event arrives")
      bucketMapState.clear()
      windowEndState.clear()
      lastActivityState.clear()
    }
    else {
      LOG.debug(s"- last activity inside of window")

      // event counts
      val bucketsToDrop = mutable.MutableList[Long]()
      var commentCount = 0
      var replyCount = 0
      var likeCount = 0
      var postCreatedInWindow = false

      val buckets = bucketMapState.iterator().asScala
      val bucketList = buckets.toList

      var futureBucketCount = 0

      val activeUserSet = mutable.Set[Long]()

      bucketList.foreach(
        entry => {
          val bucketTimestamp = entry.getKey

          if (bucketTimestamp > timestamp) {
            futureBucketCount += 1 // future bucket, ignore (count for later assertion)
          }
          else if (bucketTimestamp <= timestamp - windowSize) bucketsToDrop += bucketTimestamp // to be evicted
          else {
            // bucket within window
            val bucket = entry.getValue

            commentCount += bucket.commentCount
            replyCount += bucket.replyCount
            likeCount += bucket.likeCount
            if (bucket.originalPost) {
              postCreatedInWindow = true
            }

            // alternative (originally considered in design document):
            // MapState personId -> last activity date
            // however: events within watermark must be distinguished
            // -> more complex than id set per bucket
            // -> sets cost a bit more in terms of size and time (union)
            //    but: separate flink MapState (with unclear cost) can be avoided
            activeUserSet ++= bucket.persons
          }
        })

      if (postCreatedInWindow || (commentCount + replyCount + likeCount > 0)) {
        assert(activeUserSet.nonEmpty)

        out.collect(
          PostStatistics(
            ctx.getCurrentKey, timestamp,
            commentCount, replyCount, likeCount,
            activeUserSet.size, postCreatedInWindow))
      }
      else {
        assert(futureBucketCount > 0)
      }

      LOG.debug(s"registering FOLLOWING timer for $timestamp + $slide (current watermark: ${ctx.timerService.currentWatermark()})")

      registerWindowEndTimer(ctx.timerService, timestamp + slide)

      // evict state
      bucketsToDrop.foreach(bucketMapState.remove)
    }
  }

  private def registerWindowEndTimer(timerService: TimerService, endTime: Long): Unit = {
    windowEndState.update(endTime)
    timerService.registerEventTimeTimer(endTime)
  }

  override def processElement(value: Event,
                              ctx: KeyedProcessFunction[Long, Event, PostStatistics]#Context,
                              out: Collector[PostStatistics]): Unit = {
    if (windowEndState.value == 0) {
      // no window yet: register timer at event timestamp + slide
      LOG.debug(s"registering NEW timer for ${value.timestamp} + $slide (current watermark: ${ctx.timerService.currentWatermark()})")
      registerWindowEndTimer(ctx.timerService, value.timestamp + slide)
    }

    // register last activity for post
    lastActivityState.update(value.timestamp)

    val windowEnd = windowEndState.value
    val bucketTimestamp = PostStatisticsFunction.getBucketForTimestamp(value.timestamp, windowEnd, slide)

    value.eventType match {
      case EventType.Comment => updateBucket(bucketTimestamp, _.addComment(value.personId))
      case EventType.Reply => updateBucket(bucketTimestamp, _.addReply(value.personId))
      case EventType.Like => updateBucket(bucketTimestamp, _.addReply(value.personId))
      case EventType.Post => updateBucket(bucketTimestamp, _.registerPost(value.personId))

      case _ => // do nothing with posts and likes
    }

    if (value.timestamp > windowEnd) {
      LOG.debug(s"Early event, to future bucket: $value (current window: $windowEnd)")
    }
    else if (value.timestamp < windowEnd - windowSize) {
      LOG.debug(s"Late event, to past bucket: $value (current window: $windowEnd)")
    }
    else {
      LOG.debug(s"Regular event, to current bucket: $value")
    }
  }

  private def updateBucket(bucketTimestamp: Long, f: Bucket => Unit): Unit = {
    val bucket = getBucket(bucketTimestamp)
    f(bucket)
    bucketMapState.put(bucketTimestamp, bucket)
  }

  private def getBucket(bucketTimestamp: Long) = {
    if (bucketMapState.contains(bucketTimestamp))
      bucketMapState.get(bucketTimestamp)
    else {
      val newBucket = Bucket()
      bucketMapState.put(bucketTimestamp, newBucket)
      newBucket
    }
  }

  case class Bucket() {
    val persons: mutable.Set[Long] = mutable.Set()
    var commentCount: Int = 0
    var replyCount: Int = 0
    var likeCount: Int = 0
    var originalPost: Boolean = false

    def registerPost(personId: Long): Unit = {
      originalPost = true
      persons += personId
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
package org.mvrs.dspa.jobs.activeposts

import org.mvrs.dspa.jobs.activeposts.EventType.EventType

case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)

object EventType extends Enumeration {
  type EventType = Value
  val Post, Comment, Reply, Like = Value
}

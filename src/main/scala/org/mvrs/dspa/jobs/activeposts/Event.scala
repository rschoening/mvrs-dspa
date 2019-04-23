package org.mvrs.dspa.jobs.activeposts

import org.mvrs.dspa.events.EventType.EventType

case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)
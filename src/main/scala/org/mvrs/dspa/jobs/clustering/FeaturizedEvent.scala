package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.events.EventType.EventType

import scala.collection.mutable.ArrayBuffer

case class FeaturizedEvent(personId: Long, eventType: EventType, eventId: Long, features: ArrayBuffer[Double])

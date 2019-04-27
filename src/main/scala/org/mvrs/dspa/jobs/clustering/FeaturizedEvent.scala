package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.model.EventType.EventType

import scala.collection.mutable.ArrayBuffer

case class FeaturizedEvent(personId: Long, eventType: EventType, eventId: Long, timestamp: Long, features: ArrayBuffer[Double])

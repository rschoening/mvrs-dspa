package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.model.EventType.EventType

import scala.collection.mutable.ArrayBuffer

/**
  * Minimal event representation including a feature vector to be used for clustering/classifying
  *
  */
case class FeaturizedEvent(personId: Long, eventType: EventType, eventId: Long, timestamp: Long, features: ArrayBuffer[Double])

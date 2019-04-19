package org.mvrs.dspa.jobs.clustering

sealed trait ClassifiedEvent {
  val personId: Long
  val eventId: Long
  val cluster: Cluster
  val timestamp: Long
}

final case class ClassifiedComment(override val personId: Long, override val eventId: Long, override val cluster: Cluster, override val timestamp: Long)
  extends ClassifiedEvent

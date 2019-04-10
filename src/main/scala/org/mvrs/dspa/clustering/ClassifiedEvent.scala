package org.mvrs.dspa.clustering

trait ClassifiedEvent {
  val personId: Long
  val eventId: Long
  val cluster: Cluster
}

final case class ClassifiedComment(override val personId: Long, override val eventId: Long, override val cluster: Cluster)
  extends ClassifiedEvent

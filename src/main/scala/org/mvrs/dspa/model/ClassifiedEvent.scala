package org.mvrs.dspa.model

import org.mvrs.dspa.model.EventType.EventType

/**
  * An event that was assigned to a cluster in the cluster model
  */
final case class ClassifiedEvent(personId: Long,
                                 eventType: EventType,
                                 eventId: Long,
                                 cluster: Cluster,
                                 timestamp: Long)

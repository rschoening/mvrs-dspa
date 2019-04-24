package org.mvrs.dspa.model

import org.mvrs.dspa.model.EventType.EventType

final case class ClassifiedEvent(personId: Long, eventType: EventType, eventId: Long, cluster: Cluster, timestamp: Long)

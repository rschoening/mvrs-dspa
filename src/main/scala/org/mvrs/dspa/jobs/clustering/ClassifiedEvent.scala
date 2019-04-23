package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.events.EventType.EventType


final case class ClassifiedEvent(personId: Long, eventType: EventType, eventId: Long, cluster: Cluster, timestamp: Long)

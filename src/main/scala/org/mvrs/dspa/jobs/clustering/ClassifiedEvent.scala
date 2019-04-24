package org.mvrs.dspa.jobs.clustering

import org.mvrs.dspa.model.Cluster
import org.mvrs.dspa.model.EventType.EventType


final case class ClassifiedEvent(personId: Long, eventType: EventType, eventId: Long, cluster: Cluster, timestamp: Long)

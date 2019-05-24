package org.mvrs.dspa.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Utilities for interacting with Kafka
  */
package object kafka {
  def connectionProperties(servers: String): Properties = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    props
  }

  def consumerProperties(servers: String, groupId: String, readCommitted: Boolean = true): Properties = {
    val props = connectionProperties(servers)

    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    if (readCommitted) props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    props
  }
}

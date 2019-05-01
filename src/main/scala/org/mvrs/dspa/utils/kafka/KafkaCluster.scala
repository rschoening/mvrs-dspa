package org.mvrs.dspa.utils.kafka

import org.apache.kafka.clients.admin._
import org.mvrs.dspa.utils.kafka
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class KafkaCluster(val servers: String) {
  private val connectionProperties = kafka.connectionProperties(servers)
  private lazy val adminClient = AdminClient.create(connectionProperties)

  private val LOG = LoggerFactory.getLogger(classOf[KafkaCluster])

  def existsTopic(topicName: String, timeoutMillis: Int = 5000): Boolean = {
    val options = new ListTopicsOptions()
    options.timeoutMs(timeoutMillis)

    options.listInternal(false)

    val topicNames = adminClient.listTopics(options).names().get.asScala

    topicNames.contains(topicName)
  }

  /**
    * deletes the topic synchronously
    *
    * NOTE: on windows, deleting topics crashes the brokers!
    *
    * see https://issues.apache.org/jira/browse/KAFKA-1194 for a related issue, and a PR that has the potential to fix this,
    * awaiting approval (https://github.com/apache/kafka/pull/6329)
    */
  def deleteTopic(topicName: String, timeoutMillis: Int = 5000): Unit = {
    LOG.debug(s"deleting topic $topicName")

    val options = new DeleteTopicsOptions()
    options.timeoutMs(timeoutMillis)

    adminClient.deleteTopics(List(topicName).asJava, options).all.get // wait for all
  }

  def createTopic(topicName: String, numPartitions: Int, replicationFactor: Short, timeoutMillis: Int = 5000): Unit = {
    LOG.debug(s"creating topic $topicName")

    val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)

    val options = new CreateTopicsOptions()
    options.timeoutMs(timeoutMillis)

    adminClient.createTopics(List(newTopic).asJava, options).all.get // wait for all
  }
}

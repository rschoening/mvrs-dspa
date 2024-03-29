package org.mvrs.dspa.utils.kafka

import org.apache.kafka.clients.admin._
import org.mvrs.dspa.utils.kafka
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Helper class for operations on a kafka cluster
  *
  * @param servers string of kafka brokers
  */
class KafkaCluster(val servers: String) {
  private val connectionProperties = kafka.connectionProperties(servers)
  private lazy val adminClient = AdminClient.create(connectionProperties)

  private val LOG = LoggerFactory.getLogger(classOf[KafkaCluster])

  /**
    * Checks if a given topic exists in the cluster
    *
    * @param topicName     The topic name
    * @param timeoutMillis The timeout for the operation (milliseconds)
    * @return
    */
  def existsTopic(topicName: String, timeoutMillis: Int = 5000): Boolean = {
    val options = new ListTopicsOptions()
    options.timeoutMs(timeoutMillis)

    options.listInternal(false)

    val topicNames = adminClient.listTopics(options).names().get.asScala

    topicNames.contains(topicName)
  }

  /**
    * Deletes a topic synchronously
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

    // NOTE: this requires delete.topic.enable=true
    adminClient.deleteTopics(List(topicName).asJava, options).all.get // wait for all

    // NOTE: the future completes before the topic is fully deleted (and can be safely recreated)
    // see discussion in https://github.com/confluentinc/confluent-kafka-python/issues/524,
    // with "best" solution: https://github.com/confluentinc/confluent-kafka-python/issues/524#issuecomment-456808164

    waitForDeletion(topicName, options.timeoutMs())
  }

  /**
    * Creates a topic
    *
    * @param topicName         Topic name
    * @param numPartitions     Number of partitions
    * @param replicationFactor Replication factor
    * @param timeoutMillis     Timeout in milliseconds
    * @param overwrite         Indicates if the topic should be overwritten if it already exists
    */
  def createTopic(topicName: String,
                  numPartitions: Int,
                  replicationFactor: Short,
                  timeoutMillis: Int = 5000,
                  overwrite: Boolean = false): Unit = {
    if (overwrite && existsTopic(topicName, timeoutMillis)) deleteTopic(topicName, timeoutMillis)

    LOG.debug(s"creating topic $topicName")

    val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)

    val options = new CreateTopicsOptions()
    options.timeoutMs(timeoutMillis)

    adminClient.createTopics(List(newTopic).asJava, options).all.get // wait for all
  }

  private def waitForDeletion(topicName: String, timeoutMillis: Int, wait: Int = 1000): Unit = {
    val timedOut = System.currentTimeMillis() + timeoutMillis
    do {
      Thread.sleep(wait)
      if (!existsTopic(topicName)) return
    } while (System.currentTimeMillis() < timedOut)

    throw new Exception("Topic deletion timed out")
  }
}

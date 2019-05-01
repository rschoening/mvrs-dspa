package org.mvrs.dspa.utils

import java.util.Properties

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object KafkaUtils {
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

class KafkaTopic[T: Decoder : Encoder : TypeInformation](val name: String, val cluster: KafkaCluster)
                                                        (implicit val schemaFor: SchemaFor[T]) {

  def exists(timeoutMillis: Int = 5000): Boolean = cluster.existsTopic(name, timeoutMillis)

  /**
    * deletes the topic synchronously
    *
    * NOTE: on windows, deleting topics crashes the brokers!
    *
    * see https://issues.apache.org/jira/browse/KAFKA-1194 for a related issue, and a PR that has the potential to fix this,
    * awaiting approval (https://github.com/apache/kafka/pull/6329)
    */
  def delete(timeoutMillis: Int = 5000): Unit = cluster.deleteTopic(name, timeoutMillis)

  def create(numPartitions: Int, replicationFactor: Short, timeoutMillis: Int = 5000): Unit =
    cluster.createTopic(name, numPartitions, replicationFactor, timeoutMillis)

  def producer(partitioner: Option[FlinkKafkaPartitioner[T]] = None): FlinkKafkaProducer[T] =
    FlinkUtils.createKafkaProducer(
      name,
      cluster.servers,
      Avro4sSerializationSchema[T],
      partitioner)

  def consumer(groupId: String, readCommitted: Boolean = true): FlinkKafkaConsumer[T] =
    FlinkUtils.createKafkaConsumer(
      name,
      KafkaUtils.consumerProperties(cluster.servers, groupId, readCommitted),
      Avro4sDeserializationSchema[T])
}

class KafkaCluster(val servers: String) {
  private val connectionProperties = KafkaUtils.connectionProperties(servers)
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

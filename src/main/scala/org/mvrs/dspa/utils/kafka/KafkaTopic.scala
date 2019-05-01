package org.mvrs.dspa.utils.kafka

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema}
import org.mvrs.dspa.utils.{FlinkUtils, kafka}

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
      kafka.consumerProperties(cluster.servers, groupId, readCommitted),
      Avro4sDeserializationSchema[T])
}

package org.mvrs.dspa.utils.kafka

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema}
import org.mvrs.dspa.utils.{FlinkUtils, kafka}

class KafkaTopic[T: Decoder : Encoder : TypeInformation](val name: String, val cluster: KafkaCluster)
                                                        (implicit val schemaFor: SchemaFor[T]) {

  /**
    * Indicates if the topic exists
    *
    * @param timeoutMillis timeeout to use for the request to kafka
    * @return
    */
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

  def create(numPartitions: Int, replicationFactor: Short, timeoutMillis: Int = 5000, overwrite: Boolean = false): Unit =
    cluster.createTopic(name, numPartitions, replicationFactor, timeoutMillis, overwrite)

  /**
    * creates a producer for the topic, with Avro serialization (based on avro4s)
    *
    * @param partitioner the partitioner to use. If no partitioner is specified, the default kafka partitioner (round-robin) is used.
    *                    Depending on how the flink producer is created, the default is either flink's fixed partitioner, or
    *                    kafka's default partitioner. This method consistently uses the kafka default if left unspecified.
    * @param semantic    Defines semantic that will be used by this producer.
    * @return The Flink Kafka producer
    */
  def producer(partitioner: Option[FlinkKafkaPartitioner[T]] = None,
               semantic: FlinkKafkaProducer.Semantic = Semantic.AT_LEAST_ONCE): FlinkKafkaProducer[T] =
    FlinkUtils.createKafkaProducer(
      name,
      cluster.servers,
      Avro4sSerializationSchema[T],
      partitioner,
      semantic)

  /**
    * creates a consumer for the topic, for a given consumer group id, with Avro deserialization (based on avro4s)
    *
    * @param groupId                    the consumer group id
    * @param readCommitted              indicates if read-committed isolation is used
    * @param commitOffsetsOnCheckpoints Specifies whether or not the consumer should commit offsets back to Kafka on checkpoints.
    *                                   This setting will only have effect if checkpointing is enabled for the job
    * @return the Flink Kafka consumer
    */
  def consumer(groupId: String,
               readCommitted: Boolean = true,
               commitOffsetsOnCheckpoints: Boolean = false): FlinkKafkaConsumer[T] =
    FlinkUtils.createKafkaConsumer(
      name,
      kafka.consumerProperties(cluster.servers, groupId, readCommitted),
      Avro4sDeserializationSchema[T],
      commitOffsetsOnCheckpoints)
}

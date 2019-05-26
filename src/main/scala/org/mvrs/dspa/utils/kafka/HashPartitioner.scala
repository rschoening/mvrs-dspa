package org.mvrs.dspa.utils.kafka

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

/**
  * Hash partitioner for a type with a given hash function
  *
  * @param hash The hash function for the type
  * @tparam T The record type
  */
class HashPartitioner[T](hash: T => Long) extends FlinkKafkaPartitioner[T] {
  override def partition(record: T, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    require(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.")

    val hashCode = Math.abs(hash(record))

    val index = (hashCode % partitions.length).toInt
    partitions(index)
  }
}

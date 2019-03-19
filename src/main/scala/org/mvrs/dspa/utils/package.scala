package org.mvrs.dspa

import java.util.{Optional, Properties}

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.apache.flink.formats.avro.typeutils.AvroSerializer
//import org.apache.flink.formats.avro.{AvroDeserializationSchema, AvroRowSerializationSchema}
//import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.producer.ProducerConfig


package object utils {
  //  def createAvroKafkaProducer(topicId: String,
  //                             bootstrapServers: String,
  //                             partitioner: Option[FlinkKafkaPartitioner[Row]] = None)
  //                            (implicit env: StreamExecutionEnvironment): FlinkKafkaProducer[Row] = {
  //    val props = new Properties()
  //    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  //
  //    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
  //    val part: Optional[FlinkKafkaPartitioner[Row]] = Optional.ofNullable(partitioner.orNull)
  //
  //    val schema: Schema = SchemaBuilder.record("a").namespace("x")
  //      .fields()
  //      .name("f1").`type`("int").withDefault(1)
  //      .name("f2").`type`("int").withDefault(1)
  //      .endRecord()
  //
  //    val producer = new FlinkKafkaProducer[Row](topicId, // target topic
  //      new AvroRowSerializationSchema("") ,
  //      props, part)
  //
  //    // TODO ensure transactional writes
  //
  //    producer.setWriteTimestampToKafka(false) // TODO not sure if kafka timestamps will be of any use - deactivate for now
  //    producer
  //  }

  def createKafkaProducer[T](topicId: String,
                             bootstrapServers: String,
                             typeInfo: TypeInformation[T],
                             partitioner: Option[FlinkKafkaPartitioner[T]] = None)
                            (implicit env: StreamExecutionEnvironment): FlinkKafkaProducer[T] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
    val part: Optional[FlinkKafkaPartitioner[T]] = Optional.ofNullable(partitioner.orNull)

    val producer = new FlinkKafkaProducer[T](topicId, // target topic
      new TypeInformationSerializationSchema[T](typeInfo, env.getConfig),
      props, part)

    // TODO ensure transactional writes

    producer.setWriteTimestampToKafka(false) // TODO not sure if kafka timestamps will be of any use - deactivate for now
    producer
  }

  def createKafkaConsumer[T](topic: String, typeInfo: TypeInformation[T], props: Properties)(implicit env: StreamExecutionEnvironment): FlinkKafkaConsumer[T] = {
    val consumer = new FlinkKafkaConsumer[T](
      topic, new TypeInformationSerializationSchema[T](typeInfo, env.getConfig), props)
    consumer.setStartFromEarliest()
    consumer
  }

  def timeStampExtractor[T](maxOutOfOrderness: Time, extract: T => Long): AssignerWithPeriodicWatermarks[T] = {
    new BoundedOutOfOrdernessTimestampExtractor[T](maxOutOfOrderness) {
      override def extractTimestamp(c: T): Long = extract(c)
    }
  }
}

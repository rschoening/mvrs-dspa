package utils

import java.util.{Date, Optional, Properties}

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.test.util.AbstractTestBase
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mvrs.dspa.Settings
import org.mvrs.dspa.model.PostEvent

@Category(Array(classOf[categories.KafkaTests]))
class KafkaAvroTrials extends AbstractTestBase {
  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder


  // TODO
  // - make deserializer also serializable
  // - add test for reading
  // - compare performance with writing plain case classes - almost exactly the same
  // - compare memory consumption
  // - on deserialization: check first bytes (0 + int?),

  @Test
  def write(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val s = StringUtils.repeat("xyzä$&", 100)
    val posts =
      (0 until 500000)
        .toVector
        .map(i => PostEvent(i, i * 1000, new java.util.Date(i * 10000), None, None, None, None, Some(s), Set(1, 2, i), i, i))

    val stream = env.fromCollection(posts)

    // response time appears to be almost identical for avro and direct case class ser. (53 to 64 secs per 500000 records, running on battery)

    val serializer = Avro4sSerializationSchema[PostEvent]
    // val serializer = new TypeInformationSerializationSchema[PostEvent](createTypeInformation[PostEvent], env.getConfig)
    stream
      .addSink(createKafkaProducer("mvrs_posts", Settings.config.getString("kafka.brokers"), serializer, None))
    env.execute("test")
  }

  def createKafkaProducer[T](topicId: String,
                             bootstrapServers: String,
                             serializationSchema: SerializationSchema[T],
                             partitioner: Option[FlinkKafkaPartitioner[T]] = None)
                            (implicit env: StreamExecutionEnvironment): FlinkKafkaProducer[T] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
    val part: Optional[FlinkKafkaPartitioner[T]] = Optional.ofNullable(partitioner.orNull)

    val producer = new FlinkKafkaProducer[T](topicId, serializationSchema, props, part)

    // TODO ensure transactional writes

    producer.setWriteTimestampToKafka(false) // TODO not sure if kafka timestamps will be of any use - deactivate for now
    producer
  }
}



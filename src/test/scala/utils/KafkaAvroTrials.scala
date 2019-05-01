package utils

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mvrs.dspa.Settings
import org.mvrs.dspa.model.PostEvent
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.avro.AvroUtils.{DateDecoder, DateEncoder, DateSchemaFor}
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema}

@Category(Array(classOf[categories.KafkaTests]))
class KafkaAvroTrials extends AbstractTestBase {

  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  // TODO
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
    stream.addSink(FlinkUtils.createKafkaProducer("mvrs_posts_test", Settings.config.getString("kafka.brokers"), serializer, None))
    env.execute("test")
  }

  @Test
  def read(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val props = new Properties()
    props.setProperty("bootstrap.servers", Settings.config.getString("kafka.brokers"))

    props.setProperty("group.id", "test")
    props.setProperty("isolation.level", "read_committed")

    val stream = env.addSource(FlinkUtils.createKafkaConsumer("mvrs_posts_test", props, Avro4sDeserializationSchema[PostEvent]))

    val start = System.currentTimeMillis()

    stream.map(_ => 1).countWindowAll(100000).sum(0).map(_ => System.currentTimeMillis() - start).print

    // -> ~ 130000 rows read/deserialized per second (including startup overhead, on battery; 1000000 records)

    env.execute("test")
  }
}



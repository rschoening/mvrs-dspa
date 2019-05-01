package utils

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroRowSerializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.{Ignore, Test}
import org.junit.experimental.categories.Category
import org.mvrs.dspa.Settings
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.avro.AvroUtils.{DateDecoder, DateEncoder, DateSchemaFor}
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema, AvroUtils}

@Category(Array(classOf[categories.KafkaTests]))
class KafkaAvroTrials extends AbstractTestBase {

  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  // TODO
  // - compare performance with writing plain case classes - almost exactly the same
  // - compare memory consumption
  // - on deserialization: check first bytes (0 + int?),

  @Ignore("requires Kafka broker on default port, topic mvrs_posts_test")
  @Test
  def write(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val stream = testStream(env)

    // response time appears to be almost identical for avro and direct case class ser. (53 to 64 secs per 500000 records, running on battery)

    val serializer = Avro4sSerializationSchema[TestPostEvent]
    // val serializer = new TypeInformationSerializationSchema[PostEvent](createTypeInformation[PostEvent], env.getConfig)
    stream.addSink(FlinkUtils.createKafkaProducer("mvrs_posts_test", Settings.config.getString("kafka.brokers"), serializer, None))
    env.execute("test")
  }

  @Ignore("requires Kafka broker on default port, topic mvrs_posts_test")
  @Test
  def writeViaRow(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val stream = testStream(env)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //    tableEnv
    //      .connect(new Kafka())
    //      .withFormat(new AvroOutputFormat[]())
    val table: Table = tableEnv.fromDataStream(stream)
    // table.printSchema()

    val schema = AvroUtils.getSchema[TestPostEvent]

    table.printSchema()
    println(schema.toString(true))

    // table.javaStream.print
    table.javaStream.addSink(FlinkUtils.createKafkaProducer(
      "mvrs_posts_test",
      Settings.config.getString("kafka.brokers"),
      new AvroRowSerializationSchema(schema.toString),
      None))

    env.execute("test")

    // NOTE: Avro4s-based serialization (scala case class --> GenericRecord --> Byte array) is significantly (~30%)
    // faster than detour via Table (--> Row) and AvroRowSerializationSchema (--> GenericRecord --> Byte array)
    // also, AvroRowSerializationSchema does not support serializing Set[Int] (which gets represented as such in Row schema)
    // and: converting to Table does not support java.util.Date, instead java.sql.Timestamp would have to be used
    //
    // An alternative would be to use generated avro classes - but personal preference is not to use generated code
    // except when there are significant benefits, which here does not seem to be the case
    //
    // --> avro serde is done using Avro4s:
    // - fast
    // - broad type support/extensibility
    // - concise, no generated code
  }

  @Ignore("requires Kafka broker on default port, topic mvrs_posts_test. NOTE: test does not terminate, reading from kafka topic\"")
  @Test
  def read(): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val props = new Properties()
    props.setProperty("bootstrap.servers", Settings.config.getString("kafka.brokers"))

    props.setProperty("group.id", "test")
    props.setProperty("isolation.level", "read_committed")

    val stream = env.addSource(FlinkUtils.createKafkaConsumer("mvrs_posts_test", props, Avro4sDeserializationSchema[TestPostEvent]))

    val start = System.currentTimeMillis()

    stream.map(_ => 1).countWindowAll(100000).sum(0).map(_ => System.currentTimeMillis() - start).print

    // -> ~ 130000 rows read/deserialized per second (including startup overhead, on battery; 1000000 records)

    env.execute("test")
  }

  private def testStream(env: StreamExecutionEnvironment) = {
    val s = StringUtils.repeat("xyzä$&", 100)
    val posts =
      (0 until 500000)
        .toVector
        .map(i => TestPostEvent(i, i * 1000, new java.sql.Timestamp(i * 10000), None, None, None, None, Some(s), i, i))

    env.fromCollection(posts)
  }
}

// NOTE: not serializable if nested in test class!

sealed trait TestTimestampedEvent {
  val creationDate: java.sql.Timestamp
  val timestamp: Long = creationDate.getTime
}

sealed trait TestForumEvent extends TestTimestampedEvent {
  val personId: Long
  val postId: Long
}

final case class TestPostEvent(postId: Long,
                           personId: Long,
                           creationDate: java.sql.Timestamp,
                           imageFile: Option[String],
                           locationIP: Option[String],
                           browserUsed: Option[String],
                           language: Option[String],
                           content: Option[String],
                           // NOTE Set not supported by AvroRowSerializationSchema, exclude for preliminary test
                           //                              tags: Set[Int], // requires custom cell decoder
                           forumId: Long,
                           placeId: Int) extends TestForumEvent {
}


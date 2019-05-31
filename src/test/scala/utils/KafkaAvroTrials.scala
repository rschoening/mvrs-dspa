package utils

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.AvroRowSerializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.experimental.categories.Category
import org.junit.{Ignore, Test}
import org.mvrs.dspa.Settings
import org.mvrs.dspa.utils.FlinkUtils
import org.mvrs.dspa.utils.avro.AvroUtils
import org.mvrs.dspa.utils.avro.AvroUtils.{DateDecoder, DateEncoder, DateSchemaFor}
import org.mvrs.dspa.utils.kafka.{KafkaCluster, KafkaTopic}

@Category(Array(classOf[categories.KafkaTests]))
class KafkaAvroTrials extends AbstractTestBase {

  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  val cluster = new KafkaCluster(Settings.config.getString("kafka.brokers"))

  // TODO
  // - compare memory consumption
  // - on deserialization: check first bytes (0 + int?),

  @Ignore("requires Kafka broker on default port, topic mvrs_posts_test")
  @Test
  def write(): Unit = {
    val topic = new KafkaTopic[TestPostEvent]("mvrs_posts_test", cluster)
    if (!topic.exists()) topic.create(3, 1)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val stream = testStream(env)

    // response time appears to be almost identical for avro and direct case class ser. (53 to 64 secs per 500000 records, running on battery)

    // val serializer = new TypeInformationSerializationSchema[PostEvent](createTypeInformation[PostEvent], env.getConfig)
    stream.addSink(topic.producer())
    env.execute("test")
  }

  @Ignore("requires Kafka broker on default port, topic mvrs_posts_test")
  @Test
  def writeViaRow(): Unit = {
    val topic = new KafkaTopic[TestPostEvent]("mvrs_posts_test", cluster)
    if (!topic.exists()) topic.create(3, 1)

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
      topic.name, topic.cluster.servers,
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
    val topic = new KafkaTopic("mvrs_posts_test", cluster)
    if (!topic.exists()) topic.create(3, 1)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    val stream = env.addSource(topic.consumer("test"))

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



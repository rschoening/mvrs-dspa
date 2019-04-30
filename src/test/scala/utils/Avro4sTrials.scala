package utils

import java.io.{ByteArrayOutputStream, File}
import java.util.Date

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.mvrs.dspa.model._
import org.scalatest.{FlatSpec, Matchers}

class Avro4sTrials extends FlatSpec with Matchers {

  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  "avro4s" can "create schema for case classes" in {
    println(AvroSchema[Event].toString)
    println(AvroSchema[RawCommentEvent].toString)
    println(AvroSchema[CommentEvent].toString)
    println(AvroSchema[PostEvent].toString)
    println(AvroSchema[LikeEvent].toString)

    println(AvroSchema[PostStatistics].toString)

    println(AvroSchema[ClassifiedEvent].toString)
    println(AvroSchema[ClusterModel].toString)
    println(AvroSchema[Point].toString)
    println(AvroSchema[PostFeatures].toString)
  }

  "avro4s-based de/serializer" can "serialize fast enough" in {
    val serializer = Avro4sSerializationSchema[PostEvent]
    val deserializer = new Avro4sDeserializationSchema[PostEvent]()

    // test performance
    val start = System.currentTimeMillis()

    val s = StringUtils.repeat("xyzä$&", 100)
    val deserialized =
      (0 until 1000000)
        .toVector
        .map(i => serializer.serialize(
          PostEvent(i, i * 1000, new java.util.Date(i * 10000), None, None, None, None, Some(s), Set(1, 2, i), i, i)))
        .map(bytes => deserializer.deserialize(bytes))

    val end = System.currentTimeMillis()
    val millis = end - start

    println(s"$millis ms for ${deserialized.length} records (per record: ${millis * 1.0 / deserialized.length} ms)")

    println(deserialized.head)
    println(deserialized(deserialized.length - 1))
  }

  "avro4s" can "serialize PostEvent to/from file" in {
    val postEvent = PostEvent(1000, 10, new java.util.Date(1234), None, None, None, None, Some("blah"), Set(), 100, 10)

    val schema: Schema = AvroSchema[PostEvent]


    val file = File.createTempFile("avrotest", ".avro")
    val os: AvroOutputStream[PostEvent] = AvroOutputStream.data[PostEvent].to(file).build(schema)
    os.write(postEvent)
    os.flush()
    os.close()

    println(file.getAbsolutePath)

    val is = AvroInputStream.data[PostEvent].from(file).build(schema)
    val readPostEvent: PostEvent = is.iterator.next()
    is.close()

    // equality check not working with java.sql.Date
    // java.util.Date not (directly) supported with avro4s - didn't get encoders to work (doc seems outdated)
    // native serialization not working with LocalDateTime
    // --> use only Long timestamp in event classes, format on demand
    //     HOWEVER: how to trigger the csv encoder (kantan), if there is no specific target type?
    //     --> try again to make avro encoding/decoding work with java.util.Date
    //         --> works
    assert(postEvent.equals(readPostEvent))

    file.delete()
  }
}

class Avro4sDeserializationSchema[T: Decoder : TypeInformation](implicit schemaFor: SchemaFor[T])
  extends AbstractDeserializationSchema[T](createTypeInformation[T]) {

  @transient val readerSchema: Schema = AvroSchema[T]

  @transient private lazy val streamBuilder: AvroInputStreamBuilder[T] = AvroInputStream.binary[T]

  override def deserialize(message: Array[Byte]): T = {
    // TODO: determine writer schema id based on first byte, same as confluent registry
    // TODO: how is this id actually written? by kafka itself?
    val avroInputStream =
    streamBuilder
      .from(message)
      .build(readerSchema)

    try avroInputStream.iterator.next()
    finally avroInputStream.close()
  }
}

class Avro4sSerializationSchema1[T: Encoder](implicit schemaFor: SchemaFor[T])
  extends SerializationSchema[T] {

  @transient var writerSchema: Schema = _
  @transient private var byteStream: ByteArrayOutputStream = _
  @transient private var avroOutputStream: AvroOutputStream[T] = _

  override def serialize(element: T): Array[Byte] = {
    if (writerSchema == null) writerSchema = AvroSchema[T]
    if (byteStream == null) byteStream = new ByteArrayOutputStream()
    else byteStream.reset()

    if (avroOutputStream == null)
      avroOutputStream =
        AvroOutputStream
          .binary[T]
          .to(byteStream)
          .build(writerSchema)

    avroOutputStream.write(element)
    avroOutputStream.flush() // flush, don't close

    byteStream.toByteArray // return byte array
  }
}

class DateSchemaFor extends SchemaFor[Date] {
  override val schema: Schema = Schema.create(Schema.Type.LONG)
}

class DateEncoder extends Encoder[Date] {
  override def encode(value: Date, schema: Schema): AnyRef = value.getTime.asInstanceOf[AnyRef]
}

class DateDecoder extends Decoder[Date] {
  override def decode(value: Any, schema: Schema): Date = new Date(value.asInstanceOf[Long])
}

class Avro4sSerializationSchema[T: Encoder](schemaString: String)
  extends SerializationSchema[T] {
  require(schemaString != null)

  @transient private var schema: Schema = _
  @transient private var byteStream: ByteArrayOutputStream = _
  @transient private var avroOutputStream: AvroOutputStream[T] = _

  def writerSchema: Schema =
    if (schema == null) {
      schema = new Schema.Parser().parse(schemaString)
      schema
    }
    else schema

  override def serialize(element: T): Array[Byte] = {
    ensureInitialized()

    avroOutputStream.write(element)
    avroOutputStream.flush() // flush, don't close

    byteStream.toByteArray // return byte array
  }

  private def ensureInitialized(): Unit = {
    if (byteStream == null) byteStream = new ByteArrayOutputStream()
    else byteStream.reset()

    if (avroOutputStream == null)
      avroOutputStream =
        AvroOutputStream
          .binary[T]
          .to(byteStream)
          .build(writerSchema)
  }
}

object Avro4sSerializationSchema {
  def apply[T: Encoder](implicit schemaFor: SchemaFor[T]): Avro4sSerializationSchema[T] = {
    new Avro4sSerializationSchema[T](schemaFor.schema.toString())
  }
}

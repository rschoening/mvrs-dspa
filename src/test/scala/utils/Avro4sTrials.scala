package utils

import java.io.File

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.mvrs.dspa.model._
import org.mvrs.dspa.utils.avro.AvroUtils._
import org.mvrs.dspa.utils.avro.{Avro4sDeserializationSchema, Avro4sSerializationSchema}
import org.scalatest.{FlatSpec, Matchers}

class Avro4sTrials extends FlatSpec with Matchers {

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
    val deserializer = Avro4sDeserializationSchema[PostEvent]

    // test performance
    val start = System.currentTimeMillis()

    val s = StringUtils.repeat("xyzä$&", 100)
    val deserialized =
      (0 until 100000)
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

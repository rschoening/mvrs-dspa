package org.mvrs.dspa.utils.avro

import com.sksamuel.avro4s.{AvroInputStream, AvroInputStreamBuilder, Decoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

class Avro4sDeserializationSchema[T: Decoder : TypeInformation](schemaJSon: String)
  extends AbstractDeserializationSchema[T](createTypeInformation[T]) {

  @transient private var schema: Schema = _
  @transient private var streamBuilder: AvroInputStreamBuilder[T] = _

  def readerSchema: Schema =
    if (schema == null) {
      schema = AvroUtils.parseSchema(schemaJSon)
      schema
    }
    else schema

  override def deserialize(message: Array[Byte]): T = {
    // TODO: determine writer schema id based on first byte, same as confluent registry
    // TODO: how is this id actually written? by kafka itself?
    ensureInitialized()

    val avroInputStream =
      streamBuilder
        .from(message)
        .build(readerSchema)

    try avroInputStream.iterator.next()
    finally avroInputStream.close()
  }

  private def ensureInitialized(): Unit = {
    if (streamBuilder == null) {
      streamBuilder = AvroInputStream.binary[T]
    }
  }
}

object Avro4sDeserializationSchema {
  def apply[T: Decoder : TypeInformation](implicit schemaFor: SchemaFor[T]): Avro4sDeserializationSchema[T] =
    new Avro4sDeserializationSchema[T](schemaFor.schema.toString())
}
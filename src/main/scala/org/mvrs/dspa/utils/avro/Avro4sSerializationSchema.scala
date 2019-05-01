package org.mvrs.dspa.utils.avro

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroOutputStream, Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.SerializationSchema

class Avro4sSerializationSchema[T: Encoder](schemaJSon: String)
  extends SerializationSchema[T] {
  require(schemaJSon != null)

  @transient private var schema: Schema = _
  @transient private var byteStream: ByteArrayOutputStream = _
  @transient private var avroOutputStream: AvroOutputStream[T] = _

  def writerSchema: Schema =
    if (schema == null) {
      schema = AvroUtils.parseSchema(schemaJSon)
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
  def apply[T: Encoder](implicit schemaFor: SchemaFor[T]): Avro4sSerializationSchema[T] =
    new Avro4sSerializationSchema[T](schemaFor.schema.toString())
}
package org.mvrs.dspa.utils.avro

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroOutputStream, Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.SerializationSchema

/**
  * Avro serialization schema based on [[https://github.com/sksamuel/avro4s/blob/master/README.md avro4s]]
  *
  * @param schemaJSon The JSon string with the Avro schema for the type
  * @tparam T The type to serialize
  */
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

/**
  * Companion object
  */
object Avro4sSerializationSchema {
  /**
    * Constructs a serialization schema based on an implicit SchemaFor instance for the type
    *
    * @param schemaFor Implicit value for generating an Avro schema for a Scala or Java type
    * @tparam T The type to serialize
    * @return The serialization schema
    */
  def apply[T: Encoder](implicit schemaFor: SchemaFor[T]): Avro4sSerializationSchema[T] =
    new Avro4sSerializationSchema[T](schemaFor.schema.toString())
}
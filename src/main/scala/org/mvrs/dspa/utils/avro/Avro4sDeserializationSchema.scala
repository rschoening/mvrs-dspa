package org.mvrs.dspa.utils.avro

import com.sksamuel.avro4s.{AvroInputStream, AvroInputStreamBuilder, Decoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

/**
  * Avro deserialization schema based on [[https://github.com/sksamuel/avro4s/blob/master/README.md avro4s]]
  *
  * @param schemaJSon The JSon string with the Avro schema for the type
  * @tparam T The type to deserialize
  */

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
    // NOTE: can be extended to work with schema registry -> determine writer schema id based on first byte, same as confluent registry
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

/**
  * Companion object
  */
object Avro4sDeserializationSchema {
  /**
    * Constructs a deserialization schema based on an implicit SchemaFor instance for the type
    *
    * @param schemaFor Implicit value for generating an Avro schema for a Scala or Java type
    * @tparam T The type to deserialize
    * @return The deserialization schema
    */
  def apply[T: Decoder : TypeInformation](implicit schemaFor: SchemaFor[T]): Avro4sDeserializationSchema[T] =
    new Avro4sDeserializationSchema[T](schemaFor.schema.toString())
}
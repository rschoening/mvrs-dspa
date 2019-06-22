package org.mvrs.dspa.utils.avro

import java.util.Date

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * Utilities for avro serde based on [[https://github.com/sksamuel/avro4s/blob/master/README.md avro4s]]
  */
object AvroUtils {
  // implicit values for schema resolution by avro4s
  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  /**
    * Gets the Avro schema for a record type for which a decoder and type information is available
    *
    * @param schemaFor Implicit schemaFor instance (avro4s) for the type
    * @tparam T The record type
    * @return Avro schema
    */
  def getSchema[T: Decoder : TypeInformation](implicit schemaFor: SchemaFor[T]): Schema = schemaFor.schema

  /**
    * Parses a schema JSon string and returns the Avro schema
    *
    * @param schemaJSon Schema JSon string
    * @return Avro schema
    */
  def parseSchema(schemaJSon: String): Schema = new Schema.Parser().parse(schemaJSon)

  /**
    * SchemaFor type for java.util.Date (representing as milliseconds since epoch)
    */
  class DateSchemaFor extends SchemaFor[Date] {
    override val schema: Schema = Schema.create(Schema.Type.LONG)
  }

  /**
    * Encoder for java.util.Date -> Long
    */
  class DateEncoder extends Encoder[Date] {
    override def encode(value: Date, schema: Schema): AnyRef = value.getTime.asInstanceOf[AnyRef]
  }

  /**
    * Decoder for Long -> java.util.Date
    */
  class DateDecoder extends Decoder[Date] {
    override def decode(value: Any, schema: Schema): Date = new Date(value.asInstanceOf[Long])
  }

}






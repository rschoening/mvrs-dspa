package org.mvrs.dspa.utils.avro

import java.util.Date

import com.sksamuel.avro4s._
import org.apache.avro.Schema

object AvroUtils {
  implicit val dateSchemaFor: DateSchemaFor = new DateSchemaFor
  implicit val dateEncoder: DateEncoder = new DateEncoder
  implicit val dateDecoder: DateDecoder = new DateDecoder

  def parseSchema(schemaJSon: String): Schema = new Schema.Parser().parse(schemaJSon)

  class DateSchemaFor extends SchemaFor[Date] {
    override val schema: Schema = Schema.create(Schema.Type.LONG)
  }

  class DateEncoder extends Encoder[Date] {
    override def encode(value: Date, schema: Schema): AnyRef = value.getTime.asInstanceOf[AnyRef]
  }

  class DateDecoder extends Decoder[Date] {
    override def decode(value: Any, schema: Schema): Date = new Date(value.asInstanceOf[Long])
  }

}






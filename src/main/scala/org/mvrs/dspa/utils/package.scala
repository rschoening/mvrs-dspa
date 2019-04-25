package org.mvrs.dspa

import java.nio.file.Paths
// import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.apache.flink.formats.avro.typeutils.AvroSerializer
//import org.apache.flink.formats.avro.{AvroDeserializationSchema, AvroRowSerializationSchema}
//import org.apache.flink.types.Row

// NOTE this will be refactored, currently a mixed bag
package object utils {


  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString


}

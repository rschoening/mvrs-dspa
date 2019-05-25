package org.mvrs.dspa.functions

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

/**
  * Process function to assign the timestamp that is implicitly passed along explicitly to the record, so that is can
  * be accessed outside of a process function.
  *
  * @tparam I The record type
  */
class TimestampAssignerFunction[I] extends ProcessFunction[I, (I, Long)] {
  override def processElement(value: I,
                              ctx: ProcessFunction[I, (I, Long)]#Context,
                              out: Collector[(I, Long)]): Unit = {
    out.collect((value, ctx.timestamp()))
  }
}

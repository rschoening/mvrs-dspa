package org.mvrs.dspa.functions

import org.apache.flink.api.common.functions.AggregateFunction
import org.mvrs.dspa.utils.Default

import scala.collection.mutable

class CollectSetFunction[IN, K: Default, E](key: IN => K, value: IN => E)
  extends AggregateFunction[IN, (K, mutable.Set[E]), (K, mutable.Set[E])]() {

  override def createAccumulator(): (K, mutable.Set[E]) = (Default.value[K], mutable.Set[E]())

  override def add(v: IN, accumulator: (K, mutable.Set[E])): (K, mutable.Set[E]) =
    (key(v), accumulator._2.asInstanceOf[mutable.Set[E]] += value(v))

  override def getResult(accumulator: (K, mutable.Set[E])): (K, mutable.Set[E]) = accumulator

  override def merge(a: (K, mutable.Set[E]), b: (K, mutable.Set[E])): (K, mutable.Set[E]) =
    (a._1, a._2.asInstanceOf[mutable.Set[E]] ++= b._2)
}

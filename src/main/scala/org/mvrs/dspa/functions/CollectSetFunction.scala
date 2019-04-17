package org.mvrs.dspa.functions

import org.apache.flink.api.common.functions.AggregateFunction
import org.mvrs.dspa.utils.Default

import scala.collection.mutable

class CollectSetFunction[IN, K: Default, E](key: IN => K, value: IN => E)
  extends AggregateFunction[IN, (K, mutable.Set[E]), (K, Set[E])]() {

  type MutSet = mutable.Set[E] // type alias for readability

  override def createAccumulator(): (K, MutSet) = (Default.value[K], mutable.Set[E]())

  override def add(v: IN, acc: (K, MutSet)): (K, MutSet) = (key(v), acc._2.asInstanceOf[MutSet] += value(v))

  override def getResult(accumulator: (K, MutSet)): (K, Set[E]) = (accumulator._1, accumulator._2.toSet)

  override def merge(a: (K, MutSet), b: (K, MutSet)): (K, MutSet) = (a._1, a._2.asInstanceOf[MutSet] ++= b._2)
}

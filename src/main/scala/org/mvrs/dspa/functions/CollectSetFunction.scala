package org.mvrs.dspa.functions

import org.apache.flink.api.common.functions.AggregateFunction
import org.mvrs.dspa.utils.Default

import scala.collection.mutable

/**
  * Aggregate function to accumulate element values into sets and return a tuple of (key, set)
  *
  * @param key   function to get key of element (requires implementation of Default)
  * @param value function to get the value to be placed into the set
  * @tparam IN type of input elements
  * @tparam K  key type
  * @tparam V  value type
  */
class CollectSetFunction[IN, K: Default, V](key: IN => K, value: IN => V)
  extends AggregateFunction[IN, (K, mutable.Set[V]), (K, Set[V])]() {

  type MutSet = mutable.Set[V] // type alias for readability

  override def createAccumulator(): (K, MutSet) = (Default.value[K], mutable.Set[V]())

  override def add(v: IN, acc: (K, MutSet)): (K, MutSet) = (key(v), acc._2.asInstanceOf[MutSet] += value(v))

  override def getResult(accumulator: (K, MutSet)): (K, Set[V]) = (accumulator._1, accumulator._2.toSet)

  override def merge(a: (K, MutSet), b: (K, MutSet)): (K, MutSet) = (a._1, a._2.asInstanceOf[MutSet] ++= b._2)
}

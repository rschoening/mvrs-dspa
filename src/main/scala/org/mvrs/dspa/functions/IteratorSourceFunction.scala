package org.mvrs.dspa.functions

import org.mvrs.dspa.functions.ReplayedSourceFunction.rand
import org.mvrs.dspa.utils

class IteratorSourceFunction[T](iterator: Iterator[T],
                                extractEventTime: T => Long,
                                speedupFactor: Double,
                                maximumDelayMillis: Long,
                                delay: T => Long)
  extends ReplayedSourceFunction[T, T](identity[T], extractEventTime, speedupFactor, maximumDelayMillis, delay) {

  def this(iterator: Iterator[T],
           extractEventTime: T => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Long = 0) =
    this(iterator, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: T) => 0L
      else (_: T) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override protected def inputIterator: Iterator[T] = iterator
}

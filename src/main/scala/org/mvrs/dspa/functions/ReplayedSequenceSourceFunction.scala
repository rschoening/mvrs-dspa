package org.mvrs.dspa.functions

import org.mvrs.dspa.functions.ReplayedSourceFunction.rand
import org.mvrs.dspa.utils

class ReplayedSequenceSourceFunction[T](sequence: Seq[T],
                                        extractEventTime: T => Long,
                                        speedupFactor: Double,
                                        maximumDelayMillis: Int,
                                        delay: T => Long)
  extends ReplayedSourceFunction[T, T](identity[T], extractEventTime, speedupFactor, maximumDelayMillis, delay) {

  def this(sequence: Seq[T],
           extractEventTime: T => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Int = 0) =
    this(sequence, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: T) => 0L
      else (_: T) => utils.getNormalDelayMillis(rand, maximumDelayMilliseconds))

  override protected def inputIterator: Iterator[T] = sequence.iterator
}

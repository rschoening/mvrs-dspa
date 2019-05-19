package org.mvrs.dspa.functions

import org.mvrs.dspa.functions.ReplayedSourceFunction.rand
import org.mvrs.dspa.utils.FlinkUtils

/**
  * Source function to replay from a collection. Used in integration tests
  *
  * @param sequence the sequence to read from
  */
class ReplayedSequenceSourceFunction[T](sequence: Seq[T],
                                        extractEventTime: T => Long,
                                        speedupFactor: Double,
                                        maximumDelayMillis: Int,
                                        delay: T => Long,
                                        watermarkInterval: Int)
  extends ReplayedSourceFunction[T, T](identity[T], extractEventTime, speedupFactor, maximumDelayMillis, delay, watermarkInterval, 1000) {

  def this(sequence: Seq[T],
           extractEventTime: T => Long,
           speedupFactor: Double = 0,
           maximumDelayMilliseconds: Int = 0,
           watermarkInterval: Int = 1000) =
    this(sequence, extractEventTime, speedupFactor, maximumDelayMilliseconds,
      if (maximumDelayMilliseconds <= 0) (_: T) => 0L
      else (_: T) => FlinkUtils.getNormalDelayMillis(rand, maximumDelayMilliseconds)(),
      watermarkInterval)

  override protected def inputIterator: Iterator[T] = sequence.iterator
}

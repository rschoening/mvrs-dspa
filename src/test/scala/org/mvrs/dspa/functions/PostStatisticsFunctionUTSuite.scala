package org.mvrs.dspa.functions

import org.mvrs.dspa.activeposts.PostStatisticsFunction
import org.scalatest.{FlatSpec, Matchers}

class PostStatisticsFunctionUTSuite extends FlatSpec with Matchers {
  "bucket assignment" must "assign to expected bucket" in {
    PostStatisticsFunction.getBucketForTimestamp(5, 100, 10) should be(10)
  }

  it must "assign to proper bucket regardless of offset" in {
    PostStatisticsFunction.getBucketForTimestamp(5, 101, 10) should be(11)
  }

  it must "deal with negative timestamps" in {
    PostStatisticsFunction.getBucketForTimestamp(-50, 200, 30) should be(-10)
  }

  it must "use inclusive lower bound" in {
    PostStatisticsFunction.getBucketForTimestamp(0, 20, 10) should be(10)
  }

  it must "fail on bucket size <= 0" in {
    assertThrows[IllegalArgumentException](
      PostStatisticsFunction.getBucketForTimestamp(0, 20, 0))
  }
}

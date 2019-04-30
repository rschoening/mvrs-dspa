package functions

import org.mvrs.dspa.jobs.activeposts.PostStatisticsFunction
import org.scalatest.{FlatSpec, Matchers}

class PostStatisticsFunctionTestSuite extends FlatSpec with Matchers {
  "bucket assigner" must "assign to expected bucket" in {
    PostStatisticsFunction.getBucketForTimestamp(1, 100, 10) should be(10)
    PostStatisticsFunction.getBucketForTimestamp(5, 100, 10) should be(10)
    PostStatisticsFunction.getBucketForTimestamp(9, 100, 10) should be(10)

    PostStatisticsFunction.getBucketForTimestamp(11, 100, 10) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(15, 100, 10) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(19, 100, 10) should be(20)
  }

  it must "assign to proper bucket regardless of offset" in {
    PostStatisticsFunction.getBucketForTimestamp(5, 101, 10) should be(11)
    PostStatisticsFunction.getBucketForTimestamp(4, 105, 10) should be(5)
    PostStatisticsFunction.getBucketForTimestamp(14, 105, 10) should be(15)
  }

  it must "deal with negative timestamps" in {
    PostStatisticsFunction.getBucketForTimestamp(1, 200, 30) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(-0, 200, 30) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(-1, 200, 30) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(-39, 200, 30) should be(-10)
    PostStatisticsFunction.getBucketForTimestamp(-40, 200, 30) should be(-10)
    PostStatisticsFunction.getBucketForTimestamp(-41, 200, 30) should be(-40)
    PostStatisticsFunction.getBucketForTimestamp(-50, 200, 30) should be(-40)
    PostStatisticsFunction.getBucketForTimestamp(-80, 200, 30) should be(-70)
  }

  it must "use exclusive upper bound" in {
    PostStatisticsFunction.getBucketForTimestamp(10, 100, 10) should be(20)
    PostStatisticsFunction.getBucketForTimestamp(11, 101, 10) should be(21)
  }

  it must "use inclusive lower bound" in {
    PostStatisticsFunction.getBucketForTimestamp(0, 20, 10) should be(10)
    PostStatisticsFunction.getBucketForTimestamp(11, 21, 10) should be(21)
  }

  it must "fail on bucket size <= 0" in {
    assertThrows[IllegalArgumentException](
      PostStatisticsFunction.getBucketForTimestamp(0, 20, 0))
  }
}

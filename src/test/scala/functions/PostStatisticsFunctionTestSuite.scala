package functions

import org.mvrs.dspa.jobs.activeposts.PostStatisticsFunction._
import org.scalatest.{FlatSpec, Matchers}

class PostStatisticsFunctionTestSuite extends FlatSpec with Matchers {
  "bucket assigner" must "assign to expected bucket" in {
    getBucketForTimestamp(1, 100, 10) should be(10)
    getBucketForTimestamp(5, 100, 10) should be(10)
    getBucketForTimestamp(9, 100, 10) should be(10)

    getBucketForTimestamp(11, 100, 10) should be(20)
    getBucketForTimestamp(15, 100, 10) should be(20)
    getBucketForTimestamp(19, 100, 10) should be(20)
  }

  it must "assign to proper bucket regardless of offset" in {
    getBucketForTimestamp(5, 101, 10) should be(11)
    getBucketForTimestamp(4, 105, 10) should be(5)
    getBucketForTimestamp(14, 105, 10) should be(15)
  }

  it must "deal with negative timestamps" in {
    getBucketForTimestamp(1, 200, 30) should be(20)
    getBucketForTimestamp(-0, 200, 30) should be(20)
    getBucketForTimestamp(-1, 200, 30) should be(20)
    getBucketForTimestamp(-39, 200, 30) should be(-10)
    getBucketForTimestamp(-40, 200, 30) should be(-10)
    getBucketForTimestamp(-41, 200, 30) should be(-40)
    getBucketForTimestamp(-50, 200, 30) should be(-40)
    getBucketForTimestamp(-80, 200, 30) should be(-70)
  }

  it must "use exclusive upper bound" in {
    getBucketForTimestamp(0, 10, 10) should be(10)
    getBucketForTimestamp(10, 100, 10) should be(20)
    getBucketForTimestamp(11, 101, 10) should be(21)
  }

  it must "use inclusive lower bound" in {
    getBucketForTimestamp(0, 20, 10) should be(10)
    getBucketForTimestamp(11, 21, 10) should be(21)
  }

  it must "fail on bucket size <= 0" in {
    assertThrows[IllegalArgumentException](getBucketForTimestamp(0, 20, 0))
  }
}

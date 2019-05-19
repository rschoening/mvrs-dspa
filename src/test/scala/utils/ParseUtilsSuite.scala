package utils

import org.mvrs.dspa.utils.ParseUtils
import org.scalatest.{FlatSpec, Matchers}

class ParseUtilsSuite extends FlatSpec with Matchers {
  "zoned datetime strings" can "be parsed to milliseconds" in {
    assertResult(1000)(ParseUtils.toEpochMillis("1970-01-01T00:00:01.000Z"))
  }

  "zoned datetime strings for offset zones" can "be parsed to milliseconds" in {
    assertResult(7200000)(ParseUtils.toEpochMillis("1970-01-01T01:00:00.000-01:00")) // 2h
    assertResult(0)(ParseUtils.toEpochMillis("1970-01-01T01:00:00.000+01:00"))
  }

  "zoned datetime strings from different zones" can "be parsed to comparable timestamps" in {
    // time difference compensated by offset
    assertResult(ParseUtils.toEpochMillis("1970-01-01T00:00:00.000-02:00"))(ParseUtils.toEpochMillis("1970-01-01T01:00:00.000-01:00"))
  }

}

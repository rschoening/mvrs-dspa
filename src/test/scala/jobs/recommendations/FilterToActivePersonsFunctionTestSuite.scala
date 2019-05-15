package jobs.recommendations

import org.mvrs.dspa.jobs.recommendations.FilterToActivePersonsFunction
import org.scalatest.{FlatSpec, Matchers}

class FilterToActivePersonsFunctionTestSuite extends FlatSpec with Matchers {
  "state initialization" must "keep most recent timestamp per person" in {
    val result = FilterToActivePersonsFunction.getLastActivityByPerson(
      Seq(
        (1, 1000),
        (2, 5000),
        (1, 7000),
        (2, 4000),
        (1, 3000)
      )
    )

    assertResult(2)(result.size)
    assertResult(7000)(result(1))
    assertResult(5000)(result(2))
  }

  "state initialization" can "deal with empty state" in {
    assertResult(0)(FilterToActivePersonsFunction.getLastActivityByPerson(List()).size)
  }

  "state initialization" can "deal with single-entry state" in {
    val result = FilterToActivePersonsFunction.getLastActivityByPerson(List((1, 1000)))
    assertResult(1)(result.size)
    assertResult(1000)(result(1))
  }

}

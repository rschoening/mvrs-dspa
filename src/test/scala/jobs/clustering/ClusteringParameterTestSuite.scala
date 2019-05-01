package jobs.clustering

import org.mvrs.dspa.jobs.clustering.{ClusteringParameter, ClusteringParameterDecay, ClusteringParameterK, ClusteringParameterLabel}
import org.scalatest.{FlatSpec, Matchers}

class ClusteringParameterTestSuite extends FlatSpec with Matchers {
  "a cluster label" can "be parsed" in {
    val expected = List(Right(ClusteringParameterLabel(1, "test1")))
    assertResult(expected)(ClusteringParameter.parse("label:1=test1"))
    assertResult(expected)(ClusteringParameter.parse("label: 1=test1"))
    assertResult(expected)(ClusteringParameter.parse("label : 1 = test1"))
    assertResult(expected)(ClusteringParameter.parse(" label : 1 = test1 "))
  }

  "an empty string" must "result in an empty list" in {
    assertResult(Nil)(ClusteringParameter.parse(""))
    assertResult(Nil)(ClusteringParameter.parse(" "))
  }

  "a k value" can "be parsed" in {
    val expected = List(Right(ClusteringParameterK(2)))
    assertResult(expected)(ClusteringParameter.parse("k=2"))
    assertResult(expected)(ClusteringParameter.parse(" k = 2 "))
  }

  "a decay value" can "be parsed" in {
    val expected = List(Right(ClusteringParameterDecay(0.5)))
    assertResult(expected)(ClusteringParameter.parse("decay=0.5"))
    assertResult(expected)(ClusteringParameter.parse(" decay = 0.50 "))
  }

  "parsing errors" must "be reported" in {
    assertResult("Invalid parameter line: k")(getErrorMessage(ClusteringParameter.parse("k")))
    assertResult("For input string: \"\"")(getErrorMessage(ClusteringParameter.parse("k = ")))
    assertResult("For input string: \"a\"")(getErrorMessage(ClusteringParameter.parse("k = a")))
  }

  private def getErrorMessage(result: List[Either[Throwable, ClusteringParameter]]): String = {
    result match {
      case List(Left(e: Exception)) => e.getMessage
      case _ => ""
    }
  }
}

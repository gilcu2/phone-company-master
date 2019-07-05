package com.phone.costs

import org.scalatest.{FlatSpec, Matchers}

class PhoneCostByCustomerTest extends FlatSpec with Matchers {

  behavior of "PhoneCost"

  it should "compute the cost of a phone call" in {
    val durationsAndExpectedCosts = Seq(
      (30, 150),
      (200, 180 * 5 + 20 * 3)
    )

    durationsAndExpectedCosts.foreach { case (duration, expectedCost) => {
      PhoneCost.computeCostByCall(duration) shouldBe expectedCost
    }
    }
  }

}

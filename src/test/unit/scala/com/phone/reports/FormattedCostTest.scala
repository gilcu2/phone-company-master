package com.phone.reports

import com.phone.costs.CostByCustomer
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class FormattedCostTest extends FlatSpec with Matchers with GivenWhenThen {

  behavior of "FormattedCost"

  it should "return a formatter cost" in {

    Given("a cost")
    val cost = CostByCustomer("A", 12512)

    And("the expected result")
    val expected = FormattedCost("A", "£1.25")

    When("the cost is formatted")
    val formatted = FormattedCost(cost)

    Then("result must be the expected")
    formatted shouldBe expected

  }

}

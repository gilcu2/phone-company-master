package com.phone.logs

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PhoneLogTest extends FlatSpec with Matchers with GivenWhenThen {

  behavior of "PhoneLog"

  it should "parse correct log lines" in {

    Given("a correct line")
    val line = "A 555-333-212 00:02:03"

    And("the expected result")
    val expected = PhoneLog("A", "555-333-212", 123)

    When("the line is parsed")
    val result = PhoneLog.parse(line)

    Then("the result must be the expected")
    result shouldBe expected
  }

  it should "not parse incorrect log lines" in {

    Given("a correct line")
    val line = "A555-333-212 00:02:03"

    And("the expected result")
    val expected = PhoneLog.EmptyPhoneLog

    When("the line is parsed")
    val result = PhoneLog.parse(line)

    Then("the result must be the expected")
    result shouldBe expected
  }

}

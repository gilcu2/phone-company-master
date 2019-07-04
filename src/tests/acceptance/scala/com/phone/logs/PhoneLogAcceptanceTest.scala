package com.phone.logs

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class PhoneLogAcceptanceTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "PhoneLog"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "return the correct log lines" in {

    Given("log lines")
    val rawLines =
      """
        |A 555-333-212 00:02:03
        |B 555-433-242 00:06:41
        |C555-433-242 00:06:41
      """.computeCleanLines
    val lines = spark.createDataset(rawLines)

    And("the expected results")
    val expected = Array(
      PhoneLog("A", "555-333-212", 123),
      PhoneLog("B", "555-433-242", 401)
    )

    When("the lines are loaded")
    val results = PhoneLog.loadFromLines(lines).collect.sortBy(_.customerId)

    Then("the results must be the expected")
    results shouldBe expected

  }

}

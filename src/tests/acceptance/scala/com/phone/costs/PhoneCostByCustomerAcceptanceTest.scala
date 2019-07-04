package com.phone.costs

import com.phone.logs.PhoneLog
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import PhoneCost._

class PhoneCostByCustomerAcceptanceTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "PhoneLog"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "compute the cost per call" in {

    Given("some logs")
    val logs = spark.createDataset(Seq(
      PhoneLog("A", "3331", 200),
      PhoneLog("A", "3331", 30),
      PhoneLog("A", "3332", 20),
      PhoneLog("B", "3332", 30)
    ))

    And("the expected results")
    val expected = Set(
      CostByNumber("A", "3331", 180 * 5 + 20 * 3 + 30 * 5),
      CostByNumber("A", "3332", 20 * 5),
      CostByNumber("B", "3332", 30 * 5)
    )

    When("the cost are computed")
    val results = computeCostByNumber(logs).collect.toSet

    Then("should be the expected")
    results shouldBe expected

  }

  it should "compute the cost per customer" in {

    Given("some logs")
    val costByNumber = spark.createDataset(Seq(
      CostByNumber("A", "3331", 180 * 5 + 20 * 3 + 30 * 5),
      CostByNumber("A", "3332", 20 * 5),
      CostByNumber("B", "3332", 30 * 5)
    ))

    And("the expected results")
    val expected = Set(
      CostByCustomer("A", 20 * 5),
      CostByCustomer("B", 0)
    )

    When("the cost are computed")
    val results = computeCostByCustomer(costByNumber).collect.toSet

    Then("should be the expected")
    results shouldBe expected

  }


}

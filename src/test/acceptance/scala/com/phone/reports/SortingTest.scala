package com.phone.reports

import com.phone.costs.CostByCustomer
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper

class SortingTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Sorting"

  import spark.implicits._

  it should "sort the costs by cost" in {

    Given("costs by customer")
    val costs = spark.createDataset(Seq(
      CostByCustomer("A", 20),
      CostByCustomer("A", 30),
      CostByCustomer("B", 20),
      CostByCustomer("B", 30)
    ))

    And("the expected results")
    val expected = Array(
      CostByCustomer("A", 20),
      CostByCustomer("B", 20),
      CostByCustomer("A", 30),
      CostByCustomer("B", 30)
    )

    When("the costs are sorted")
    val sorted = Sorting.sort(costs, field = "cost", defaultField = "id").collect

    Then("the results must be the expected")
    sorted shouldBe expected

  }

  it should "sort the costs by id if the indicated field is wrong" in {

    Given("costs by customer")
    val costs = spark.createDataset(Seq(
      CostByCustomer("A", 20),
      CostByCustomer("A", 30),
      CostByCustomer("B", 30),
      CostByCustomer("B", 20)
    ))

    And("the expected results")
    val expected = Array(
      CostByCustomer("A", 20),
      CostByCustomer("A", 30),
      CostByCustomer("B", 20),
      CostByCustomer("B", 30)
    )

    When("the costs are sorted")
    val sorted = Sorting.sort(costs, field = "cast", defaultField = "id").collect

    Then("the results must be the expected")
    sorted shouldBe expected

  }

}

package com.phone.costs

import com.phone.logs.PhoneLog
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object PhoneCost {

  private val costLessThan3Minutes = 5
  private val costMoreThan3Minutes = 3
  private val secondsOf3Minutes = 180

  def compute(logs: Dataset[PhoneLog])(implicit spark: SparkSession): Dataset[CostByCustomer] = {

    val costByNumber = computeCostByNumber(logs)

    computeCostByCustomer(costByNumber)

  }

  def computeCostByCustomer(costByNumber: Dataset[CostByNumber])(implicit spark: SparkSession)
  : Dataset[CostByCustomer] = {
    import spark.implicits._

    costByNumber
      .map(costByNumber => CostAndMaximum(costByNumber.customerId, costByNumber.cost, 0L))
      .groupByKey(_.customerId)
      .reduceGroups(reduce _)
      .map { case (customerId, costAndMaximum) =>
        CostByCustomer(customerId, costAndMaximum.cost - costAndMaximum.maximum)
      }
  }

  private def reduce(c1: CostAndMaximum, c2: CostAndMaximum): CostAndMaximum =
    c1.copy(cost = c1.cost + c2.cost, maximum = c1.maximum.max(c2.maximum))

  def computeCostByNumber(logs: Dataset[PhoneLog])(implicit spark: SparkSession): Dataset[CostByNumber] = {
    import spark.implicits._

    logs
      .map(log => CostByNumber(log.customerId, log.calledNumber, computeCost(log.duration)))
      .groupBy("customerId", "number")
      .agg(sum("cost"))
      .select($"customerId", $"number", $"sum(cost)".alias("cost"))
      .as[CostByNumber]
  }

  def computeCost(duration: Int): Int = if (duration <= secondsOf3Minutes)
    duration * costLessThan3Minutes
  else
    secondsOf3Minutes * costLessThan3Minutes + (duration - secondsOf3Minutes) * costMoreThan3Minutes

  case class CostByNumber(customerId: String, number: String, cost: Long)

  case class CostAndMaximum(customerId: String, cost: Long, maximum: Long)

}

case class CostByCustomer(customerId: String, cost: Long)



package com.phone.costs

import com.phone.logs.PhoneLog
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object PhoneCost {

  private val costLessThan3Minutes = 5
  private val costMoreThan3Minutes = 3
  private val secondsOf3Minutes = 180

  def compute(logs: Dataset[PhoneLog])(implicit spark: SparkSession): Dataset[Cost] = {
    import spark.implicits._

    val costByNumber = computeCostByNumber(logs)

  }

  def computeCostByNumber(logs: Dataset[PhoneLog]): Dataset[CostByNumber] =
    logs
      .map(log => CostByNumber(log.customerId, log.calledNumber, computeCost(log.duration)))
      .groupBy("customerId", "calledNumber")
      .agg(sum("cost"))
      .select($"customerId", $"calledNumber", $"sum(cost)".alias("cost"))
      .as[CostByNumber]

  def computeCost(duration: Int): Int = if (duration <= secondsOf3Minutes)
    duration * costLessThan3Minutes
  else
    secondsOf3Minutes * costLessThan3Minutes + (duration - secondsOf3Minutes) * costMoreThan3Minutes

  case class CostByNumber(customerId: String, number: String, cost: Int)

}

case class Cost(customerId: String, cost: Int)



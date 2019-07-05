package com.phone.reports

import com.phone.costs.CostByCustomer

object FormattedCost {

  def apply(costByCustomer: CostByCustomer): FormattedCost = {
    val pounds = costByCustomer.cost / 10000
    val pennies = (costByCustomer.cost % 10000).toString.take(2)
    FormattedCost(costByCustomer.customerId, s"£$pounds.$pennies")
  }

}

case class FormattedCost(customerId: String, cost: String)
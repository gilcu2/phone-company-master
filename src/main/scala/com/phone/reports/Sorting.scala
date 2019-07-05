package com.phone.reports

import com.phone.costs.CostByCustomer
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Dataset

object Sorting extends LazyLogging {

  val fields = Map("id" -> "customerId", "cost" -> "cost")

  def sort(costs: Dataset[CostByCustomer], field: String, defaultField: String): Dataset[CostByCustomer] = {
    val primaryField = if (fields.contains(field)) fields(field) else {
      logger.warn(s"Incorrect sorting field: $field, taking default")
      fields(defaultField)
    }
    val secondaryField = (fields.values.toSet - primaryField).head
    costs.sort(primaryField, secondaryField)
  }


}

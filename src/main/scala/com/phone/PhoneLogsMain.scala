package com.phone

import com.phone.costs.PhoneCost
import com.phone.logs.PhoneLog
import com.phone.reports.{FormattedCost, Sorting}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import interfaces.Spark
import interfaces.Time._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object PhoneLogsMain extends LazyLogging {

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val sortField = opt[String](default = Some(""))
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, sortField: String)

  case class ConfigValues(inputPath: String, defaultSortField: String)

  def main(implicit args: Array[String]): Unit = {

    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")

    implicit val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("PhoneLogProcessing")
    implicit val spark = Spark.sparkSession(sparkConf)

    val configValues = getConfigValues(conf)
    val lineArguments = getLineArgumentsValues(args, configValues)

    process(configValues, lineArguments)

    val endTime = getCurrentTime
    val humanTime = getHumanDuration(beginTime, endTime)
    logger.info(s"End: $endTime Total: $humanTime")

  }

  private def process(configValues: ConfigValues, lineArguments: CommandParameterValues)(
    implicit spark: SparkSession
  ): Unit = {
    import spark.implicits._

    val logs = PhoneLog.load(configValues.inputPath)
    if (lineArguments.logCountsAndTimes) logs.cache()

    val beginTime = getCurrentTime

    val costs = PhoneCost.computeTotalCosts(logs)
    val sortedCost = Sorting.sort(costs, lineArguments.sortField, configValues.defaultSortField)
    val formattedCost = sortedCost.map(FormattedCost.apply)
    formattedCost.cache()
    val customerCount = formattedCost.count()

    if (lineArguments.logCountsAndTimes) {
      val logCount = logs.count

      val endTime = getCurrentTime
      val humanTime = getHumanDuration(beginTime, endTime)
      logger.info(s"Logs: $logCount Customers: $customerCount Processing time: $humanTime")
    }

    formattedCost.show(customerCount.toInt)

  }

  private def getConfigValues(conf: Config): ConfigValues = {
    val inputPath = conf.getString("inputPath")
    val defaultSortField = conf.getString("defaultSortField")

    ConfigValues(inputPath, defaultSortField)
  }

  private def getLineArgumentsValues(args: Array[String], configValues: ConfigValues): CommandParameterValues = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val sortField = if (parsedArgs.sortField().nonEmpty) parsedArgs.sortField() else configValues.defaultSortField

    CommandParameterValues(logCountsAndTimes, sortField)
  }

}
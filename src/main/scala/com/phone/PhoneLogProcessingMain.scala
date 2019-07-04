package com.phone

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import interfaces.Spark
import interfaces.Time._
import org.apache.spark.SparkConf
import org.rogach.scallop.ScallopConf

object PhoneLogProcessingMain extends LazyLogging {

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val sortFields = opt[String](default = Some(""))
  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, sortFields: String)

  case class ConfigValues(inputPath: String, defaultSortFields: String)

  def main(implicit args: Array[String]): Unit = {

    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")

    implicit val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("PhoneLogProcessing")
    implicit val spark = Spark.sparkSession(sparkConf)


  }

}
package com.phone.logs

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

object PhoneLog extends LazyLogging {

  private val LineRegex ="""^\s*([A-Z]+)\s+([0-9]{3}-[0-9]{3}-[0-9]{3})\s+([0-9]{2}):([0-9]{2}):([0-9]{2})\s*$""".r
  private val secondsOfHour = 3600
  private val secondsOfMin = 60
  val EmptyPhoneLog = PhoneLog("", "", 0)

  def load(path: String)(implicit spark: SparkSession): Dataset[PhoneLog] = {
    logger.info("Reading addresses from CSV")

    val lines = spark.read.textFile(path)
    loadFromLines(lines)
  }

  def loadFromLines(lines: Dataset[String])(implicit spark: SparkSession): Dataset[PhoneLog] = {
    import spark.implicits._

    lines.map(parse).filter(_ != EmptyPhoneLog)
  }

  def parse(s: String): PhoneLog = s match {
    case LineRegex(customerId, calledNumber, hours, minutes, seconds) =>
      val duration = hours.toInt * secondsOfHour + minutes.toInt * secondsOfMin + seconds.toInt
      PhoneLog(customerId, calledNumber, duration)
    case _ =>
      logger.warn(s"Invalid line: $s")
      EmptyPhoneLog
  }

}

case class PhoneLog(customerId: String, calledNumber: String, duration: Int)

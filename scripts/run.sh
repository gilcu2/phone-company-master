#!/usr/bin/env bash

CONFIGPATH="."
PROGRAM="../target/scala-2.11/phonelogs.jar"

spark-submit \
--class com.phone.PhoneLogsMain \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
$PROGRAM $*

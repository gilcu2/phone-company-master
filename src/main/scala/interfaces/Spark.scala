package interfaces

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def getTotalCores(implicit spark: SparkSession): Int = {
    val workers = spark.sparkContext.statusTracker.getExecutorInfos.size
    val cores = spark.sparkContext.getConf.get("spark.executor.cores", "1").toInt
    workers * cores
  }

}
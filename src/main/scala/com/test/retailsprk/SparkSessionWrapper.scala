package com.test.retailsprk

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {

    SparkSession
      .builder()
      .appName("Retail_App")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

  }

}

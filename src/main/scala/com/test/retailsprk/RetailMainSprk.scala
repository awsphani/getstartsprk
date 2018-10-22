package com.test.retailsprk
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
/*
*
*
spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.test.retailsprk.RetailMainSprk \
getstartsprk_2.11-0.1.jar s3://digi-ingre-preprod-east/cde/dev/member-access/tst1/retail_db/order_items/ s3://digital-ingress-preprod-east/cde/dev/member-access/tst1/retail_db/order_items_out/
*
* */

object RetailMainSprk  {

  def main(args: Array[String]) : Unit ={

    val conf = new SparkConf().setAppName("RevPerOrder")

    val sc = new SparkContext(conf)

    val orderItems =sc.textFile(args(0))

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    log.warn("RevPerOrder")

    val revenuePerOrder = orderItems
      .map(oi =>(oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
      .reduceByKey(_ + _)
      .map(oi => oi._1 + "," +oi._2)

    revenuePerOrder.saveAsTextFile(args(1))



  }


  def calcRev(src: String) : Any = {





  }


}

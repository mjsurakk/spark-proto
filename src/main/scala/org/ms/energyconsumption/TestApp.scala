package org.ms.energyconsumption

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by markku on 11/04/2017.
  */
object TestApp extends App {

  //  implicit val spark = SparkSession.builder().appName("energyconsumption").getOrCreate()
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("energyconsumption")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val rdd = sc.textFile("/Users/markku/Downloads/Piha_10-4-2017.csv").map(_.split(";")).filter(_.length == 4)
  val tuples = rdd.map(arr => {
    val timestamp = arr(0)
    val datetime = arr(1)
    val temperature = arr(2)
    val humidity = arr(3)
    (timestamp, datetime, temperature, humidity)
  })
  tuples.take(10).toList.foreach(println)

}

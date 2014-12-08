package org.apache.drill.rdd

import org.apache.drill.rdd.DrillConversions._
import org.apache.drill.rdd.complex.{CNull, CObject}
import org.apache.spark.{SparkConf, SparkContext}

object DosDemo extends App {
  val conf = new SparkConf().setAppName("DrillSql").setMaster("local")
  val sc = new SparkContext(conf)

  def queryOne(): Unit = {
    val sql = "SELECT t.revenue FROM dfs.`local`.`lil500.json` t limit 10"
    val result = sc.drillRDD(sql)
    val formatted = result.map { r =>
      val (currency, amount) = (r.revenue.currency, r.revenue.amount)
      s"$amount $currency"
    }
//    result.foreach(println)
    formatted.foreach(println)
  }

  def queryTwo(): Unit = {
    var easy = sc.parallelize(0 until 10)
    val squared = easy.map { i =>
      CObject(("index", i), ("value", i*i))
    }
    sc.registerAsTable("squared", squared)
    val sql = "select * from spark.squared where index>3 and index<10 limit 5"
    val result = sc.drillRDD(sql)
    result.foreach(println)
  }

  queryTwo
}

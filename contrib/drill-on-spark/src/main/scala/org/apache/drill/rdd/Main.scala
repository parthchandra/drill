package org.apache.drill.rdd

import org.apache.drill.rdd.DrillConversions._
import org.apache.drill.rdd.complex.{CNull, CObject}
import org.apache.spark.{SparkConf, SparkContext}


object Main extends App {
  val conf = new SparkConf().setAppName("DrillSql").setMaster("local")
  val sc = new SparkContext(conf)


  def queryJSON(): Unit = {
    val sql = "SELECT * FROM dfs.`local`.`lil500.json`"
    val employees = sc.drillRDD(sql)

    employees.foreach(println)
    val count = employees.count()
    println(s"total records found: $count")
  }

  def queryRDD(): Unit = {
    // iterate from 10 to 30 in 10 spark partitions
    val mobile = sc.parallelize(10 until 30, 10)
    val few = mobile.zipWithIndex.map { case (r, i) =>
      CObject(("index", i), ("number", r))
    }

    sc.registerAsTable("simple", few)
    val sql2 = "SELECT * FROM spark.simple t where t.index > 2 and t.index < 6 and number > 13"
    val result = sc.drillRDD(sql2)

    result.foreach { r =>
      val (index, number, value) = (r.index, r.number, r.value)
      println(s"$index -> $number :: $value")
    }
    val count = result.count
    println(s"total records found: $count")
    sc.stop()
  }

  queryRDD
}




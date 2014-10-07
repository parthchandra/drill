package org.apache.drill.rdd

import org.apache.drill.rdd.DrillConversions._
import org.apache.spark.{SparkConf, SparkContext}


object Main extends App {
  val conf = new SparkConf().setAppName("DrillSql").setMaster("local")
  val sc = new SparkContext(conf)


  def flatJSON(): Unit = {
    val sql = "SELECT * FROM cp.`employee.json`"
    val employees = sc.drillRDD(sql)
    val fullNames = employees.map(r=>{
      val first = r.first_name.value
      val last = r.last_name.value
      s"$first $last"
    })
    fullNames.foreach(println)
    val count = fullNames.count()
    println(s"total records found: $count")

  }

  def complexJSON(): Unit = {
    val sql = "SELECT * FROM dfs.`/Users/hgunes/workspaces/mapr/incubator-drill/data/mobile-small.json`"
    val rdd0 = sc.drillRDD(sql).cache()
    val rdd1 = rdd0.zipWithIndex.map {
      case (r, i) =>
//        val ui = r.user_info
//        s"$ui"
        val (tid, date, cid) = (r.trans_id, r.date, r.user_info.cust_id) // r.date, r.user_info.cust_id)
        s"$i $tid $date $cid"
    }.cache
    val count = rdd1.count
    println(rdd1.first)
    println(s"total records found: $count")
    sc.stop()
  }

  complexJSON
}




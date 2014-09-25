import org.apache.drill.rdd.DrillConversions
import org.apache.spark.{SparkContext, SparkConf}

import DrillConversions._


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
    val rdd0 = sc.drillRDD(sql)
    val rdd1 = rdd0.zipWithIndex.map {
      case (r, i) =>
//        val ui = r.user_info
//        s"$ui"
        val (tid, date, cid) = (r.trans_id, r.date, r.user_info.cust_id) // r.date, r.user_info.cust_id)
        s"$i $tid $date $cid"
    }
    val count = rdd1.count
    rdd1.foreach(println)
    println(s"total records found: $count")
  }

  complexJSON
}




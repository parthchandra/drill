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
    val count = fullNames.count()
    fullNames.foreach(println)
    println(s"total records found: $count")

  }

  flatJSON
}




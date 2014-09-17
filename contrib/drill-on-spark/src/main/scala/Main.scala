import java.lang.StringBuilder
import java.util.Properties
import java.util.concurrent.{TimeUnit, Future}

import org.apache.drill.exec.client.DrillClient
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint
import org.apache.drill.exec.proto.UserBitShared
import org.apache.drill.exec.proto.UserBitShared.QueryId
import org.apache.drill.exec.rpc.RpcException
import org.apache.drill.exec.rpc.user.{ConnectionThrottle, QueryResultBatch, UserResultsListener}
import org.apache.drill.rdd.DrillConversions
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

//import org.apache.spark.sql.json.JsonRDD

import DrillConversions._


object Main extends App {
  println(getClass.getClassLoader.getResource("io/netty/buffer/PooledByteBufAllocator.class"))
  println(getClass.getClassLoader.getResource("org/apache/log4j/spi/RootLogger.class"))
  println(getClass.getClassLoader.getResource("org/apache/log4j/Category.class"))
  println(getClass.getClassLoader.getResource("org/apache/log4j/Logger.class"))
  println(getClass.getClassLoader.getResource("org/apache/hadoop/conf/Configuration.class"))

  val conf = new SparkConf().setAppName("DrillSql").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlCtx = new SQLContext(sc)

  //"select name from sys.options"
  val sql = "SELECT * FROM cp.`employee.json` LIMIT 10"
  val drill = sc.drillRDD(sql)
  drill.map(r=>r(0)).collect().foreach(println)
  println(drill)
}




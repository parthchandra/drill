package org.apache.drill.rdd

import java.util.concurrent.TimeUnit

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.rdd.query.StreamingQueryEngine
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SchemaRDD, SQLContext}

import scala.concurrent.duration.{Duration, FiniteDuration}

object DrillConversions {

  private val engineFactory = {
    () =>
      val conf = DrillConfig.createClient()
      val allocator = new TopLevelAllocator(conf)
      new StreamingQueryEngine[SparkRowType](allocator)
  }

  implicit class DrillSparkContext(sc: SparkContext) {

    def drillRDD(sql: String): DrillRDD = {
      new DrillRDD(sc, sql, engineFactory)
    }
  }

  implicit class DrillSqlContext(sqlCtx: SQLContext) {

    def drillRDD(sql: String): SchemaRDD = {
      new DrillRDD(sqlCtx.sparkContext, sql, engineFactory)
      null
    }
  }

  implicit class RichTime(value:Long) {
    def seconds(): Duration = new FiniteDuration(value, TimeUnit.SECONDS)
    def millis(): Duration = new FiniteDuration(value, TimeUnit.MILLISECONDS)
  }

}

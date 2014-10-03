package org.apache.drill.spark.rdd

import java.io.Serializable
import java.util.concurrent.TimeUnit

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.complex.reader.BaseReader
import org.apache.drill.exec.vector.complex.reader.BaseReader.{ListReader, MapReader}
import org.apache.drill.spark.sql._
import org.apache.drill.spark.sql.query.{QueryContext, StreamingQueryManager}
import org.apache.drill.spark.sql.sql.{DrillRecord, RecordInfo}
import org.apache.drill.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag

object DrillConversions {

  /*
   This method is called once per partition during compute phase.
  */
  type IN = DrillIncomingRowType
  type OUT = DrillOutgoingRowType


  private def managerFactory[IN:ClassTag] = () => {
    val conf = DrillConfig.createClient()
    val allocator = new TopLevelAllocator(conf)
    val loader = new RecordBatchLoader(allocator)
    new StreamingQueryManager[IN](QueryContext[IN](loader, recordFactory[IN]))
  }

  private def recordFactory[IN:ClassTag] = (info:RecordInfo) => {
    new DrillRecord(info).asInstanceOf[IN]
  }

  val registry = new RDDRegistry[OUT]

  implicit class RichSparkContext(sc: SparkContext) {

    def getRegistry() = registry

    def drillRDD(sql: String): DrillRDD[IN, OUT] = {
      new DrillRDD[IN, OUT](sc, DrillContext(sql, managerFactory, registry, true))
    }

    def registerAsTable(name:String, rdd:RDD[OUT]): Unit = {
      registry.register(name, rdd)
    }
  }

  implicit class DrillReader(reader:BaseReader) {
    def :>(name:String): MapReader = {
      reader.asInstanceOf[MapReader].reader(name).asInstanceOf[MapReader]
    }

    def ::>(name:String): ListReader = {
      reader.asInstanceOf[MapReader].reader(name).asInstanceOf[ListReader]
    }

    def /(name: String): BaseReader = {
      reader.asInstanceOf[MapReader].reader(name)
    }
  }

  implicit class RichTime(value:Long) {
    def seconds(): Duration = new FiniteDuration(value, TimeUnit.SECONDS)
    def millis(): Duration = new FiniteDuration(value, TimeUnit.MILLISECONDS)
  }

}
